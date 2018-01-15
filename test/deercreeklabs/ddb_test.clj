(ns deercreeklabs.ddb-test
  (:require
   [clojure.core.async :as ca]
   [clojure.test :refer [deftest is]]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.ddb-utils :as du]
   [deercreeklabs.log-utils :as lu :refer [debugs]]
   [schema.core :as s]
   [taoensso.timbre :as timbre :refer [debugf errorf infof]]))

(du/configure-logging)

;; Use this instead of fixtures, which are hard to make work w/ async testing.
(s/set-fn-validation! true)
#_
(deftest ^:the-one test-distributed-lock
  (au/test-async
   60000
   (ca/go
     (let [lock-name "test-app-primary"
           l1-ch (ca/chan)
           l2-ch (ca/chan)
           on-acq1 #(ca/put! l1-ch :got-lock)
           on-rel1 #(ca/put! l1-ch :released-lock)
           on-acq2 #(ca/put! l2-ch :got-lock)
           on-rel2 #(ca/put! l2-ch :released-lock)
           l1 (ddb/make-distributed-lock lock-name 5000 on-acq1 on-rel1)
           l2 (ddb/make-distributed-lock lock-name 5000 on-acq2 on-rel2)]
       (try
         (let [[v ch] (au/alts? [l1-ch l2-ch (ca/timeout 1000)])
               _ (is (= l1-ch ch))
               _ (is (= :got-lock v))
               _ (ddb/release l1)
               [v ch] (au/alts? [l1-ch l2-ch (ca/timeout 1000)])
               _ (is (= l1-ch ch))
               _ (is (= :released-lock v))
               [v ch] (au/alts? [l1-ch l2-ch (ca/timeout 1000)])
               _ (is (= l2-ch ch))
               _ (is (= :got-lock v))])
         (finally
           (ddb/stop l1)
           (ddb/stop l2)))))))

(deftest test-get-set
  (au/test-async
   5000
   (ca/go
     (let [client (du/make-ddb-client)
           table-name "ddb-test"
           m {:part "a" :sort 123 :value "Foo"}
           ret-ch (du/<ddb-set client table-name m)
           [ret ch] (au/alts? [ret-ch (ca/timeout 1000)])
           _ (is (= ret-ch ch))
           _ (is (true? ret))
           k (select-keys m [:part :sort])
           ret-ch (du/<ddb-get client table-name k)
           [ret ch] (au/alts? [ret-ch (ca/timeout 1000)])
           _ (is (= ret-ch ch))
           _ (is (= m ret))]))))
