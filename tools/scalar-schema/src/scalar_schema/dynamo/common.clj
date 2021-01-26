(ns scalar-schema.dynamo.common
  (:require [scalar-schema.common :as common])
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

(defn get-index-name
  [schema key-name]
  (str (get-table-name schema) \. INDEX_NAME_PREFIX \. key-name))

(defn get-global-index-name
  [schema key-name]
  (str (get-table-name schema) \. GLOBAL_INDEX_NAME_PREFIX \. key-name))
