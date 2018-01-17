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
   (java.nio ByteBuffer)
   (java.security SecureRandom)))

(def default-refresh-to-check-ratio 5)
(def lock-table-name "locks")

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
    (ca/put! ret-ch e)))

(defn make-cbs [ret-ch op-name]
  [(make-success-cb ret-ch)
   (make-failure-cb ret-ch op-name)])

(defn make-handler
  ([success-cb failure-cb op-name]
   (make-handler success-cb failure-cb op-name identity identity))
  ([success-cb failure-cb op-name transform-result]
   (make-handler success-cb failure-cb op-name transform-result identity))
  ([success-cb failure-cb op-name transform-result transform-error]
   (reify AsyncHandler
     (onSuccess [this req result]
       (try
         (success-cb (transform-result result))
         (catch Exception e
           (try
             (failure-cb e)
             (catch Exception e
               (errorf "Error calling failure-cb for %s: \n%s" op-name
                       (lu/get-exception-msg-and-stacktrace e)))))))
     (onError [this e]
       (try
         (failure-cb (transform-error e))
         (catch Exception e
           (errorf "Error calling failure-cb for %s: \n%s"
                   op-name (lu/get-exception-msg-and-stacktrace e))))))))

(defn make-boolean-handler [success-cb failure-cb op-name]
  (make-handler success-cb failure-cb op-name (constantly true) identity))

(defn ddb-get
  ([client table-name key-map success-cb failure-cb]
   (ddb-get client table-name key-map success-cb failure-cb true))
  ([^AmazonDynamoDBAsyncClient client table-name key-map
    success-cb failure-cb consistent?]
   (try
     (let [am (clj-map->attr-map key-map)
           transform-result (fn [result]
                              (-> (.getItem ^GetItemResult result)
                                  (attr-map->clj-map)))
           handler (make-handler success-cb failure-cb "ddb-get"
                                 transform-result)]
       (.getItemAsync client table-name am consistent? handler))
     (catch Exception e
       (try
         (failure-cb e)
         (catch Exception e
          (errorf "Error calling failure-cb for ddb-get: %s"
                  (lu/get-exception-msg-and-stacktrace e))))))))

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
  (try
    (let [^java.util.Map item (clj-map->attr-map m)
          handler (make-boolean-handler success-cb failure-cb "ddb-put")]
      (.putItemAsync client table-name item ^AsyncHandler handler))
    (catch Exception e
      (try
        (failure-cb e)
        (catch Exception e
          (errorf "Error calling failure-cb for ddb-put: %s"
                  (lu/get-exception-msg-and-stacktrace e)))))))

(defn <ddb-put [client table-name m]
  (let [ret-ch (ca/chan)
        [success-cb failure-cb] (make-cbs ret-ch "<ddb-put")]
    (ddb-put client table-name m success-cb failure-cb)
    ret-ch))

(defn ddb-delete
  [^AmazonDynamoDBAsyncClient client ^String table-name key-map
   success-cb failure-cb]
  (try
    (let [^java.util.Map am (clj-map->attr-map key-map)
          handler (make-boolean-handler success-cb failure-cb "ddb-delete")]
      (.deleteItemAsync client table-name am ^AsyncHandler handler))
    (catch Exception e
      (try
        (failure-cb e)
        (catch Exception e
          (errorf "Error calling failure-cb for ddb-delete: %s"
                  (lu/get-exception-msg-and-stacktrace e)))))))

(defn <ddb-delete [^AmazonDynamoDBAsyncClient client ^String table-name key-map]
  (let [ret-ch (ca/chan)
        [success-cb failure-cb] (make-cbs ret-ch "<ddb-delete")]
    (ddb-delete client table-name key-map success-cb failure-cb)
    ret-ch))

(defn op-type-dispatch [cond-expr & args]
  (when cond-expr
    (let [op (first cond-expr)]
      (cond
        (#{:= :not= :<> :< :> :<= :>=} op) :comparison
        (#{:exists :not-exists} op) :existential
        :else (throw (ex-info (str "Unknown operator: " op)
                              (sym-map op cond-expr)))))))

(defmulti cond-expr->string op-type-dispatch)
(defmulti cond-expr->attrs-and-values op-type-dispatch)

(defmethod cond-expr->string :comparison
  [cond-expr attr->alias value->alias]
  (let [[op attr v] cond-expr
        op->str {:= "="
                 :not= "<>"
                 :<> "<>"
                 :< "<"
                 :> ">"
                 :<= "<="
                 :>= ">="}]
    (string/join " " [(attr->alias (name attr))
                      (op->str op)
                      (value->alias v)])))

(defmethod cond-expr->string :existential
  [cond-expr attr->alias value->alias]
  (let [[op attr] cond-expr
        op->str {:exists "attribute_exists"
                 :not-exists "attribute_not_exists"}]
    (str (op->str op) "(" (attr->alias (name attr)) ")")))

(defmethod cond-expr->string nil
  [cond-expr attr->alias value->alias]
  nil)

(defmethod cond-expr->attrs-and-values :comparison
  [cond-expr]
  (let [[op attr v] cond-expr]
    [[attr] [v]]))

(defmethod cond-expr->attrs-and-values :existential
  [cond-expr]
  (let [[op attr] cond-expr]
    [[attr] []]))

(defmethod cond-expr->attrs-and-values nil
  [cond-expr]
  [[] []])

(defn make-update-expression-data [update-map cond-expr]
  (let [[ce-attrs ce-vals] (cond-expr->attrs-and-values cond-expr)
        um-attrs (keys update-map)
        um-vals (vals update-map)
        attrs (set (concat ce-attrs um-attrs))
        vals (set (concat ce-vals um-vals))
        attr-info (reduce (fn [acc [i attr]]
                            (let [a-alias (str "#attr" i)
                                  attr (name attr)]
                              (-> acc
                                  (update :attr->alias assoc attr a-alias)
                                  (update :alias->attr assoc a-alias attr))))
                          {:attr->alias {}
                           :alias->attr {}}
                          (map-indexed vector attrs))
        val-info (reduce (fn [acc [i value]]
                             (let [v-alias (str ":val" i)]
                               (-> acc
                                   (update :value->alias assoc value v-alias)
                                   (update :alias->value assoc v-alias value))))
                           {:value->alias {}
                            :alias->value {}}
                           (map-indexed vector vals))
        {:keys [attr->alias alias->attr]} attr-info
        {:keys [value->alias alias->value]} val-info
        make-update-pair (fn [[attr value]]
                           (let [a-alias (attr->alias (name attr))
                                 v-alias (value->alias value)]
                             (str a-alias " = " v-alias)))
        uestr (->> update-map
                   (map make-update-pair)
                   (string/join ",")
                   (str "SET "))
        cestr (cond-expr->string cond-expr attr->alias value->alias)]
    (sym-map alias->attr alias->value uestr cestr)))

(defn ddb-update
  [^AmazonDynamoDBAsyncClient client table-name key-map
   update-map cond-expr success-cb failure-cb]
  (try
    (let [akm (clj-map->attr-map key-map)
          transform-result (constantly true)
          transform-error (fn [e]
                            (if (instance? ConditionalCheckFailedException e)
                              false
                              e))
          handler (make-handler success-cb failure-cb "ddb-update"
                                transform-result transform-error)
          data (make-update-expression-data update-map cond-expr)
          {:keys [alias->attr alias->value uestr cestr]} data
          uir (doto (UpdateItemRequest.)
                (.setTableName table-name)
                (.setKey akm)
                (.setExpressionAttributeValues (clj-map->attr-map
                                                alias->value))
                (.setUpdateExpression uestr))]
      (doseq [[alias attr-name] alias->attr]
        (.addExpressionAttributeNamesEntry uir alias attr-name))
      (when cestr
        (.setConditionExpression uir cestr))
      (.updateItemAsync client uir handler))
    (catch Exception e
      (try
        (failure-cb e)
        (catch Exception e
          (errorf "Error calling failure-cb for ddb-update: %s"
                  (lu/get-exception-msg-and-stacktrace e)))))))

(defn <ddb-update
  ([client table-name key-map update-map]
   (<ddb-update client table-name key-map update-map nil))
  ([client table-name key-map update-map cond-expr]
   (let [ret-ch (ca/chan)
         [success-cb failure-cb] (make-cbs ret-ch "<ddb-update")]
     (ddb-update client table-name key-map update-map cond-expr
                 success-cb failure-cb)
     ret-ch)))

(defn ^AmazonDynamoDBAsyncClient make-ddb-client []
  (AmazonDynamoDBAsyncClientBuilder/defaultClient))

;; (defn make-lease-id []
;;   (let [rng (SecureRandom.)
;;         bytes (byte-array 8)]
;;     (.nextBytes rng bytes)
;;     (ba/byte-array->b64 bytes)))

;; (defprotocol IDistributedLockClient
;;   (acquired? [this])
;;   (stop [this])
;;   (start-aquire-loop* [this]))

;; (defrecord DistributedLockClient
;;     [ddb-client lock-name actor-name lease-length-ms on-acquire on-release
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
;;     (let [owner actor-name
;;           lease-id (make-lease-id)
;;           k (sym-map lock-name)
;;           update-map (sym-map owner lease-id lease-length-ms)
;;           cond-expr [:not-exists :lock-name]]
;;       (<ddb-update ddb-client lock-table-name k update-map cond-expr)))

;;   (<refresh-lock* [this]
;;     (debugf "%s: Entering <refresh-lock*" actor-name)
;;     (au/go
;;       (ca/<! (ca/timeout (/ lease-length-ms refresh-ratio)))
;;       (let [update-map {:}
;;             cond-expr]
;;         (au/<? (<ddb-update ddb-client lock-table-name k
;;                             update-map cond-expr)))))

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
;;                ddb-client lock-name actor-name lease-length-ms on-acquire on-release
;;                refresh-ratio client *acquired? *shutdown?)]
;;      (start-aquire-loop* lock)
;;      lock)))
