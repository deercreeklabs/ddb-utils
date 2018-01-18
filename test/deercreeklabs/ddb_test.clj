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


(deftest test-distributed-locks
  (au/test-async
   60000
   (ca/go
     (let [lock-name "test-app-primary"
           lease-length-ms 3000
           lch (ca/chan)
           on-acq1 #(ca/put! lch :c1-got-lock)
           on-rel1 #(ca/put! lch :c1-released-lock)
           on-acq2 #(ca/put! lch :c2-got-lock)
           on-rel2 #(ca/put! lch :c2-released-lock)
           c1 (du/make-distributed-lock-client
               lock-name "c1" lease-length-ms on-acq1 on-rel1)
           _ (ca/<! (ca/timeout (* 1.5 lease-length-ms)))
           c2 (du/make-distributed-lock-client
               lock-name "c2" lease-length-ms on-acq2 on-rel2)]
       (try
         (let [[v ch] (au/alts? [lch (ca/timeout 1000)])
               _ (is (= lch ch))
               _ (is (= :c1-got-lock v))
               _ (du/stop c1)
               [v ch] (au/alts? [lch (ca/timeout 1000)])
               _ (is (= lch ch))
               _ (is (= :c1-released-lock v))
               _ (ca/<! (ca/timeout (* 1.5 lease-length-ms)))
               [v ch] (au/alts? [lch (ca/timeout 1000)])
               _ (is (= lch ch))
               _ (is (= :c2-got-lock v))])
         (finally
           (du/stop c1)
           (du/stop c2)))))))

(deftest test-get-put-delete
  (au/test-async
   5000
   (ca/go
     (let [client (du/make-ddb-client)
           table-name "ddb-test"
           m {:part "a" :sort 123 :value "Foo"}
           ret-ch (du/<ddb-put client table-name m)
           [ret ch] (au/alts? [ret-ch (ca/timeout 1000)])
           _ (is (= ret-ch ch))
           _ (is (true? ret))
           k (select-keys m [:part :sort])
           ret-ch (du/<ddb-get client table-name k)
           [ret ch] (au/alts? [ret-ch (ca/timeout 1000)])
           _ (is (= ret-ch ch))
           _ (is (= m ret))
           ret-ch (du/<ddb-delete client table-name k)
           [ret ch] (au/alts? [ret-ch (ca/timeout 1000)])
           _ (is (= ret-ch ch))
           _ (is (true?  ret))
           ret-ch (du/<ddb-get client table-name k)
           [ret ch] (au/alts? [ret-ch (ca/timeout 1000)])
           _ (is (= ret-ch ch))
           _ (is (nil? ret))]))))

(deftest test-update
  (au/test-async
   5000
   (ca/go
     (let [client (du/make-ddb-client)
           table-name "ddb-test"
           m {:part "b" :sort 789 :value "Foo"}

           ret-ch (du/<ddb-put client table-name m)
           [ret ch] (au/alts? [ret-ch (ca/timeout 1000)])
           _ (is (= ret-ch ch))
           _ (is (true? ret))

           k (select-keys m [:part :sort])
           new-value "Bar"
           new-fruit "Apple"
           ret-ch (du/<ddb-update client table-name k
                                  {:value new-value
                                   :fruit new-fruit})
           [ret ch] (au/alts? [ret-ch (ca/timeout 1000)])
           _ (is (= ret-ch ch))
           _ (is (true? ret))

           ret-ch (du/<ddb-get client table-name k)
           [ret ch] (au/alts? [ret-ch (ca/timeout 1000)])
           _ (is (= ret-ch ch))
           _ (is (= new-value (:value ret)))
           _ (is (= new-fruit (:fruit ret)))

           newer-fruit "Pear"
           ret-ch (du/<ddb-update client table-name k
                                  {:fruit newer-fruit}
                                  [:= :value "Foo"])
           [ret ch] (au/alts? [ret-ch (ca/timeout 1000)])
           _ (is (= ret-ch ch))
           _ (is (false? ret))

           ret-ch (du/<ddb-update client table-name k
                                  {:fruit newer-fruit}
                                  [:= :value "Bar"])
           [ret ch] (au/alts? [ret-ch (ca/timeout 1000)])
           _ (is (= ret-ch ch))
           _ (is (true? ret))

           ret-ch (du/<ddb-update client table-name k
                                  {:fruit "Tomato"}
                                  [:not-exists :part])
           [ret ch] (au/alts? [ret-ch (ca/timeout 1000)])
           _ (is (= ret-ch ch))
           _ (is (false? ret))

           ret-ch (du/<ddb-get client table-name k)
           [ret ch] (au/alts? [ret-ch (ca/timeout 1000)])
           _ (is (= ret-ch ch))
           _ (is (= {:fruit "Pear", :part "b", :sort 789, :value "Bar"} ret))

           k2 {:part "a" :sort 123}
           v2 {:yo "dawg"}
           ret-ch (du/<ddb-update client table-name k2 v2
                                  [:not-exists :part])
           [ret ch] (au/alts? [ret-ch (ca/timeout 1000)])
           _ (is (= ret-ch ch))
           _ (is (true? ret))

           ret-ch (du/<ddb-get client table-name k2)
           [ret ch] (au/alts? [ret-ch (ca/timeout 1000)])
           _ (is (= ret-ch ch))
           _ (is (= (merge k2 v2) ret))

           ret-ch (du/<ddb-delete client table-name k)
           [ret ch] (au/alts? [ret-ch (ca/timeout 1000)])
           _ (is (= ret-ch ch))
           _ (is (true?  ret))
           ]))))

(deftest ^:the-one test-query
  (au/test-async
   5000
   (ca/go
     (let [client (du/make-ddb-client)
           table-name "ddb-test"
           m {:part "myvals" :sort 1 :value "Foo"}
           k (select-keys m [:part :sort])

           ret-ch (du/<ddb-put client table-name m)
           [ret ch] (au/alts? [ret-ch (ca/timeout 1000)])
           _ (is (= ret-ch ch))
           _ (is (true? ret))

           ret-ch (du/<ddb-put client table-name (assoc m :sort 2))
           [ret ch] (au/alts? [ret-ch (ca/timeout 1000)])
           _ (is (= ret-ch ch))
           _ (is (true? ret))

           opts {:limit 1
                 :reverse? true}
           ret-ch (du/<ddb-query client table-name (select-keys m [:part]) opts)
           [ret ch] (au/alts? [ret-ch (ca/timeout 1000)])
           _ (is (= ret-ch ch))
           _ (is (= 1 (count ret)))
           highest (first ret)
           _ (is (= 2 (:sort highest)))

           opts {:limit 1}
           ret-ch (du/<ddb-query client table-name (select-keys m [:part]) opts)
           [ret ch] (au/alts? [ret-ch (ca/timeout 1000)])
           _ (is (= ret-ch ch))
           _ (is (= 1 (count ret)))
           lowest (first ret)
           _ (is (= 1 (:sort lowest)))
           ]))))
