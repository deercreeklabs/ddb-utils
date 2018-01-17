(ns deercreeklabs.ddb-utils
  (:require
   [clojure.core.async :as ca]
   [clojure.string :as string]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.log-utils :as lu :refer [debugs]]
   [taoensso.timbre :as timbre :refer [debugf errorf infof]])
  (:import
   (com.amazonaws ClientConfiguration)
   (com.amazonaws.handlers AsyncHandler)
   (com.amazonaws.services.dynamodbv2 AmazonDynamoDBAsyncClient
                                      AmazonDynamoDBAsyncClientBuilder)
   (com.amazonaws.services.dynamodbv2.model AttributeValue
                                            AttributeValueUpdate
                                            ConditionalCheckFailedException
                                            GetItemRequest
                                            GetItemResult
                                            PutItemResult
                                            UpdateItemRequest
                                            UpdateItemResult)
   (java.nio ByteBuffer)))

(def default-refresh-to-check-ratio 5)

(defmacro sym-map
  "Builds a map from symbols.
   Symbol names are turned into keywords and become the map's keys.
   Symbol values become the map's values.
  (let [a 1
        b 2]
    (sym-map a b))  =>  {:a 1 :b 2}"
  [& syms]
  (zipmap (map keyword syms) syms))

(defn configure-logging
  ([] (configure-logging :debug))
  ([level]
   (timbre/merge-config!
    {:level level
     :output-fn lu/short-log-output-fn
     :appenders
     {:println {:ns-blacklist []}}})))

(defn get-current-time-ms
  []
  (System/currentTimeMillis))

(defn- parse-number
  [^String ns]
  (if (.contains ns ".")
    (Double/parseDouble ns)
    (Long/parseLong ns)))

(defn get-value [av]
  (let [[type val] (some #(when-not (nil? (val %)) %)
                         (dissoc (bean av) :class))]
    (case type
      (:s :b :BOOL) val
      (:SS :BS) (into #{} val)
      :n (parse-number val)
      :NS (into #{} (map parse-number val))
      :l (into [] (map get-value val))
      :m (into {} (map (fn [[k v]] [k (get-value v)]) val))
      :NULL nil)))

(defn attr-map->clj-map [am]
  (when am
    (reduce-kv (fn [acc k av]
                 (assoc acc (keyword k)
                        (get-value av)))
               {} (into {} am))))

(defn clj-key->str-key [k]
  (name k))

(defn clj-value->attr-value [value]
  (let [attr (AttributeValue.)]
    (cond
      (nil? value) (.setNULL attr true)
      (number? value) (.setN attr (str value))
      (string? value) (.setS attr value)
      (instance? Boolean value) (.setBOOL attr value)
      (sequential? value) (.setL attr
                                 (map clj-value->attr-value value))
      (ba/byte-array? value) (.setB attr (ByteBuffer/wrap value))
      (instance? ByteBuffer value) (.setB attr value)
      (set? value) (.setSS attr (map name value))
      :else (throw (ex-info
                    (str "Value of type " (class value)
                         " is not supported")
                    (sym-map value))))
    attr))

(defn clj-map->attr-map [m]
  (reduce-kv (fn [acc k v]
               (assoc acc (clj-key->str-key k)
                      (clj-value->attr-value v)))
             {} m))

(defn make-success-cb [ret-ch]
  (fn [result]
    (if (nil? result)
      (ca/close! ret-ch)
      (ca/put! ret-ch result))))

(defn make-failure-cb [ret-ch op-name]
  (fn [e]
    (errorf "%s error: \n%s" op-name (lu/get-exception-msg-and-stacktrace e))
    (ca/put! ret-ch e)))

(defn make-cbs [ret-ch op-name]
  [(make-success-cb ret-ch)
   (make-failure-cb ret-ch op-name)])

(defn make-boolean-handler [success-cb failure-cb op-name]
  (reify AsyncHandler
    (onError [this e]
      (try
        (failure-cb e)
        (catch Exception e
          (errorf "Error calling failure-cb for %s: \n%s"
                  op-name (lu/get-exception-msg-and-stacktrace e)))))
    (onSuccess [this req result]
      (try
        (success-cb true)
        (catch Exception e
          (try
            (failure-cb e)
            (errorf "Error calling failure-cb for %s: \n%s" op-name
                    (lu/get-exception-msg-and-stacktrace e))))))))

(defn ddb-get
  ([client table-name key-map success-cb failure-cb]
   (ddb-get client table-name key-map success-cb failure-cb true))
  ([^AmazonDynamoDBAsyncClient client table-name key-map
    success-cb failure-cb consistent?]
   (let [am (clj-map->attr-map key-map)
         handler (reify AsyncHandler
                   (onError [this e]
                     (failure-cb e))
                   (onSuccess [this req result]
                     (try
                       (-> (.getItem ^GetItemResult result)
                           (attr-map->clj-map)
                           (success-cb))
                       (catch Exception e
                         (failure-cb e)))))]
     (.getItemAsync client table-name am consistent? handler))))

(defn <ddb-get
  ([client table-name key-map]
   (<ddb-get client table-name key-map true))
  ([client table-name key-map consistent?]
   (let [ret-ch (ca/chan)
         [success-cb failure-cb] (make-cbs ret-ch "<ddb-get")]
     (ddb-get client table-name key-map success-cb failure-cb consistent?)
     ret-ch)))

(defn ddb-put
  [^AmazonDynamoDBAsyncClient client ^String table-name m success-cb failure-cb]
  (let [^java.util.Map item (clj-map->attr-map m)
        handler (make-boolean-handler success-cb failure-cb "ddb-put")]
    (.putItemAsync client table-name item ^AsyncHandler handler)))

(defn <ddb-put [client table-name m]
  (let [ret-ch (ca/chan)
        [success-cb failure-cb] (make-cbs ret-ch "<ddb-put")]
    (ddb-put client table-name m success-cb failure-cb)
    ret-ch))

(defn ddb-delete
  [^AmazonDynamoDBAsyncClient client ^String table-name key-map
   success-cb failure-cb]
  (let [^java.util.Map am (clj-map->attr-map key-map)
        handler (make-boolean-handler success-cb failure-cb "ddb-delete")]
    (.deleteItemAsync client table-name am ^AsyncHandler handler)))

(defn <ddb-delete [^AmazonDynamoDBAsyncClient client ^String table-name key-map]
  (let [ret-ch (ca/chan)
        [success-cb failure-cb] (make-cbs ret-ch "<ddb-delete")]
    (ddb-delete client table-name key-map success-cb failure-cb)
    ret-ch))

(defn ddb-update
  [^AmazonDynamoDBAsyncClient client table-name key-map
   update-map condition-exprs success-cb failure-cb]
  (let [akm (clj-map->attr-map key-map)
        handler (reify AsyncHandler
                  (onError [this e]
                    (try
                      (if (instance? ConditionalCheckFailedException e)
                        (failure-cb false)
                        (failure-cb e))
                      (catch Exception e
                        (errorf "Error calling failure-cb for ddb-update: \n%s"
                                (lu/get-exception-msg-and-stacktrace e)))))
                  (onSuccess [this req result]
                    (try
                      (success-cb true)
                      (catch Exception e
                        (try
                          (failure-cb e)
                          (errorf
                           "Error calling failure-cb for ddb-update: \n%s"
                           (lu/get-exception-msg-and-stacktrace e)))))))
        expr-vecs (map-indexed (fn [i [k v]]
                                 (let [ean (str "#attr" i)
                                       eav (str ":val" i)
                                       eanv [ean (name k)]
                                       eavm {eav v}
                                       ue (str ean " = " eav)]
                                   [eanv eavm ue]))
                               update-map)
        eanvs (map first expr-vecs)
        eavms (map second expr-vecs)
        uestr (->> (map #(nth % 2) expr-vecs)
                   (string/join ",")
                   (str "SET "))
        uir (doto (UpdateItemRequest.)
              (.setTableName table-name)
              (.setKey akm)
              (.setExpressionAttributeValues (clj-map->attr-map
                                              (apply merge eavms)))
              (.setUpdateExpression uestr))]
    (doseq [[alias attr-name] eanvs]
      (.addExpressionAttributeNamesEntry uir alias attr-name))
    (.updateItemAsync client uir handler)))

(defn <ddb-update
  ([client table-name key-map update-map]
   (<ddb-update client table-name key-map update-map nil))
  ([client table-name key-map update-map condition-exprs]
   (let [ret-ch (ca/chan)
         [success-cb failure-cb] (make-cbs ret-ch "<ddb-update")]
     (ddb-update client table-name key-map update-map condition-exprs
                 success-cb failure-cb)
     ret-ch)))

(defn ^AmazonDynamoDBAsyncClient make-ddb-client []
  (AmazonDynamoDBAsyncClientBuilder/defaultClient))

;; (defprotocol IDistributedLockClient
;;   (acquired? [this])
;;   (stop [this])
;;   (start-aquire-loop* [this]))

;; (defrecord DistributedLockClient
;;     [lock-name actor-name lease-length-ms on-acquire on-release
;;      refresh-ratio client *acquired? *shutdown?]
;;   IDistributedLockClient
;;   (acquired? [this]
;;     @*acquired?)

;;   (stop [this]
;;     (when (acquired? this)
;;       (on-release this))
;;     (reset! *shutdown? true))

;;   (<attempt-acquisition* [this lease-id lease-length-ms]
;;     (debugf "%s: Entering <attempt-acquisition*" actor-name)
;;     (au/go
;;       (ca/<! (ca/timeout lease-length-ms))
;;       ))

;;   (<create-lock* [this]
;;     (au/go
;;       ))

;;   (<refresh-lock* [this]
;;     (debugf "%s: Entering <refresh-lock*" actor-name)
;;     (au/go
;;       (ca/<! (ca/timeout (/ lease-length-ms refresh-ratio)))
;;       (au/<? ())))

;;   (start-aquire-loop* [this]
;;     (ca/go
;;       (try
;;         (while-not @*shutdown
;;           (let [k (u/sym-map lock-name)
;;                 info (or (au/<? (<ddb-get client locks-table-name k))
;;                          (au/<? (<create-lock* this)))
;;                 {:keys [owner lease-length-ms lease-id]} info]
;;             (if (= actor-name owner)
;;               (au/<? (<refresh-lock* this))
;;               (au/<? (attempt-acquisition* this lease-id lease-length-ms)))))
;;         (catch Exception e
;;           (errorf "Error in start-aquire-loop*: %s"
;;                   (lu/get-exception-msg-and-stacktrace e)))))))

;; (defn make-distributed-lock-client
;;   ([lock-name actor-name lease-length-ms on-acquire on-release]
;;    (make-distributed-lock
;;     lock-name actor-name lease-length-ms on-acquire on-release
;;     default-refresh-ratio))
;;   ([lock-name actor-name lease-length-ms on-acquire on-release
;;     refresh-ratio]
;;    (let [^AmazonDynamoDBAsyncClient ddb-client
;;          (AmazonDynamoDBAsyncClientBuilder/defaultClient)
;;          *acquired? (atom false)
;;          *shutdown? (atom false)
;;          lock (->DistributedLock
;;                lock-name actor-name lease-length-ms on-acquire on-release
;;                refresh-ratio client *acquired? *shutdown?)]
;;      (start-aquire-loop* lock)
;;      lock)))
