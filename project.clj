(defproject deercreeklabs/ddb-utils "0.1.5-SNAPSHOT"
  :description "Utilities for working with DynamoDB"
  :license {:name "Apache License, Version 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0"}
  :lein-release {:scm :git
                 :deploy-via :clojars}

  :pedantic? :abort

  :profiles
  {:dev
   {:global-vars {*warn-on-reflection* true}
    :source-paths ["dev" "src"]
    :plugins
    [[lein-ancient "0.6.15"
      :exclusions [org.apache.httpcomponents/httpclient
                   com.amazonaws/aws-java-sdk-s3 commons-logging commons-codec]]
     [lein-cloverage "1.0.11" :exclusions [org.clojure/clojure
                                           org.clojure/tools.reader]]
     ;; Because of confusion with a defunct project also called
     ;; lein-release, we exclude lein-release from lein-ancient.
     [lein-release "1.0.9" :upgrade false
      :exclusions [org.clojure/clojure]]
     [s3-wagon-private "1.3.2" :exclusions [commons-logging]]]
    :dependencies
    [[org.clojure/tools.namespace "0.2.11"]]
    :repl-options {:init-ns user}}
   :uberjar {:aot :all}}

  :dependencies
  [[com.amazonaws/aws-java-sdk-dynamodb "1.11.361"
    :exclusions [com.fasterxml.jackson.core/jackson-core commons-logging
                 joda-time]]
   [com.taoensso/timbre "4.10.0" :exclusions [org.clojure/tools.reader]]
   [commons-logging/commons-logging "1.2"]
   [deercreeklabs/async-utils "0.1.12"]
   [deercreeklabs/baracus "0.1.10"]
   [deercreeklabs/log-utils "0.1.8"]
   [org.clojure/core.async "0.4.474"]
   [org.clojure/clojure "1.9.0"]
   [prismatic/schema "1.1.9"]]

  :test-selectors {:default (complement :slow)
                   :the-one :the-one
                   :slow :slow
                   :all (constantly true)})
