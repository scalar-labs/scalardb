(ns scalar-schema.dynamo.common
  (:require [clojure.string :as str]
            [scalar-schema.common :as common])
  (:import (software.amazon.awssdk.auth.credentials AwsBasicCredentials
                                                    StaticCredentialsProvider)))

(def ^:const ^:private ^String INDEX_NAME_PREFIX "index")
(def ^:const ^:private ^String GLOBAL_INDEX_NAME_PREFIX "global_index")

(defn get-credentials-provider
  [user password]
  (StaticCredentialsProvider/create
   (AwsBasicCredentials/create user password)))

(defn get-table-name
  [{:keys [database table]}]
  (common/get-fullname database table))

(defn- get-table-name-without-prefix
  [{:keys [prefix] :as schema}]
  (let [table-name (get-table-name schema)]
    (if prefix (str/replace-first table-name (java.util.regex.Pattern/compile (str prefix \_)) "")
        table-name)))

(defn get-index-name
  [schema key-name]
  (str (get-table-name-without-prefix schema)
       \. INDEX_NAME_PREFIX \. key-name))

(defn get-global-index-name
  [schema key-name]
  (str (get-table-name-without-prefix schema)
       \. GLOBAL_INDEX_NAME_PREFIX \. key-name))
