(ns deercreeklabs.ddb-utils
  (:require
   [clojure.core.async :as ca]
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
  (reduce-kv (fn [acc k av]
               (assoc acc (keyword k)
                      (get-value av)))
             {} (into {} am)))

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

(defn ddb-get
  ([client table-name clj-key-map success-cb failure-cb]
   (ddb-get client table-name clj-key-map success-cb failure-cb true))
  ([^AmazonDynamoDBAsyncClient client table-name clj-key-map
    success-cb failure-cb consistent?]
   (let [am (clj-map->attr-map clj-key-map)
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
  ([client table-name clj-key-map]
   (<ddb-get client table-name clj-key-map true))
  ([client table-name clj-key-map consistent?]
   (let [ret-ch (ca/chan)
         success-cb (fn [result]
                      (ca/put! ret-ch result))
         failure-cb (fn [e]
                      (errorf "<ddb-get error: \n%s"
                              (lu/get-exception-msg-and-stacktrace e))
                      (ca/put! ret-ch e))]
     (ddb-get client table-name clj-key-map success-cb failure-cb consistent?)
     ret-ch)))

(defn ddb-set
  [^AmazonDynamoDBAsyncClient client ^String table-name m success-cb failure-cb]
  (let [^java.util.Map item (clj-map->attr-map m)
        handler (reify AsyncHandler
                  (onError [this e]
                    (failure-cb e))
                  (onSuccess [this req result]
                    (try
                      (success-cb true)
                      (catch Exception e
                        (failure-cb e)))))]
    (.putItemAsync client table-name item ^AsyncHandler handler)))

(defn <ddb-set [client table-name m]
  (let [ret-ch (ca/chan)
        success-cb (fn [result]
                     (ca/put! ret-ch result))
        failure-cb (fn [e]
                     (errorf "<ddb-set error: \n%s"
                             (lu/get-exception-msg-and-stacktrace e))
                     (ca/put! ret-ch e))]
    (ddb-set client table-name m success-cb failure-cb)
    ret-ch))

(defn ^AmazonDynamoDBAsyncClient make-ddb-client []
  (AmazonDynamoDBAsyncClientBuilder/defaultClient))

;; (defprotocol IDistributedLock
;;   (acquired? [this])
;;   (release [this])
;;   (stop [this])
;;   (start-aquire-loop* [this]))

;; (defrecord DistributedLock
;;     [lock-name actor-name timeout-ms on-acquire on-release
;;      refresh-to-check-ratio client *acquired? *shutdown?]
;;   IDistributedLock
;;   (acquired? [this]
;;     @*acquired?)
;;   (release [this]
;;     ...)
;;   (stop [this]
;;     (when (acquired? this)
;;       (release this))
;;     (reset! *shutdown? true))
;;   (start-aquire-loop* [this]
;;     (ca/go
;;       (try
;;         (while-not (@*shutdown?)
;;           (let [k (u/sym-map lock-name)
;;                 ret (au/<? (<ddb-get client locks-table-name k))
;;                 {:keys [owner last-refresh-time-ms]} ret
;;                 now (u/get-current-time-ms)]
;;             (if (and (= actor-name owner)))))
;;         (catch Exception e
;;           (errorf "Error in start-aquire-loop*: %s"
;;                   (lu/get-exception-msg-and-stacktrace e)))))))

;; (defn make-distributed-lock
;;   ([lock-name actor-name timeout-ms on-acquire on-release]
;;    (make-distributed-lock
;;     lock-name actor-name timeout-ms on-acquire on-release
;;     default-refresh-to-check-ratio))
;;   ([lock-name actor-name timeout-ms on-acquire on-release
;;     refresh-to-check-ratio])
;;   (let [^AmazonDynamoDBAsyncClient ddb-client
;;         (AmazonDynamoDBAsyncClientBuilder/defaultClient)
;;         *acquired? (atom false)
;;         *shutdown? (atom false)
;;         lock (->DistributedLock
;;               lock-name actor-name timeout-ms on-acquire on-release
;;               refresh-to-check-ratio client *acquired? *shutdown?)]
;;     (start-aquire-loop* lock)
;;     lock))
