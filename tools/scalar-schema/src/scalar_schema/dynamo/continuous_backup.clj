(ns scalar-schema.dynamo.continuous-backup
  (:require [scalar-schema.dynamo.common :as dynamo])
  (:import (software.amazon.awssdk.services.dynamodb.model UpdateContinuousBackupsRequest
                                                                                 PointInTimeRecoverySpecification)))

(defn- get-pitr-spec
  []
  (-> (PointInTimeRecoverySpecification/builder)
      (.pointInTimeRecoveryEnabled true)
      .build))

(defn- get-update-backup-request
  [schema]
  (-> (UpdateContinuousBackupsRequest/builder)
      (.tableName (dynamo/get-table-name schema))
      (.pointInTimeRecoverySpecification (get-pitr-spec))
      .build))

(defn enable-continuous-backup
  [client schema]
  (->> (get-update-backup-request schema)
       (.updateContinuousBackups client)))
