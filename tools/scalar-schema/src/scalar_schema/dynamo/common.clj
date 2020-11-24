(ns scalar-schema.dynamo.common
  (:require [scalar-schema.common :as common])
  (:import (software.amazon.awssdk.auth.credentials AwsBasicCredentials
                                                    StaticCredentialsProvider)))

(defn get-credentials-provider
  [user password]
  (StaticCredentialsProvider/create
   (AwsBasicCredentials/create user password)))

(defn get-table-name
  [{:keys [database table]}]
  (common/get-fullname database table))
