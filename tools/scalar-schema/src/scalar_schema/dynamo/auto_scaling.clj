(ns scalar-schema.dynamo.auto-scaling
  (:require [clojure.tools.logging :as log]
            [scalar-schema.common :as common])
  (:import (software.amazon.awssdk.auth.credentials AwsBasicCredentials
                                                    StaticCredentialsProvider)
           (software.amazon.awssdk.regions Region)
           (software.amazon.awssdk.services.applicationautoscaling
            ApplicationAutoScalingClient)
           (software.amazon.awssdk.services.applicationautoscaling.model
            DeregisterScalableTargetRequest
            RegisterScalableTargetRequest
            ObjectNotFoundException
            ScalableDimension
            ServiceNamespace)))

(defn- get-credentials-provider
  [user password]
  (StaticCredentialsProvider/create
   (AwsBasicCredentials/create user password)))

(defn get-scaling-client
  [user password region]
  (-> (ApplicationAutoScalingClient/builder)
      (.credentialsProvider (get-credentials-provider user password))
      (.region (Region/of region))
      .build))

(defn- make-scaling-request
  [table ru type]
  (-> (RegisterScalableTargetRequest/builder)
      (.serviceNamespace ServiceNamespace/DYNAMODB)
      (.resourceId (str "table/" table))
      (.scalableDimension (if (= type :read)
                            ScalableDimension/DYNAMODB_TABLE_READ_CAPACITY_UNITS
                            ScalableDimension/DYNAMODB_TABLE_WRITE_CAPACITY_UNITS))
      (.minCapacity (int (if (> ru 10) (/ ru 10) ru)))
      (.maxCapacity ru)
      .build))

(defn- make-scaling-disable-request
  [table type]
  (-> (DeregisterScalableTargetRequest/builder)
      (.serviceNamespace ServiceNamespace/DYNAMODB)
      (.resourceId (str "table/" table))
      (.scalableDimension (if (= type :read)
                            ScalableDimension/DYNAMODB_TABLE_READ_CAPACITY_UNITS
                            ScalableDimension/DYNAMODB_TABLE_WRITE_CAPACITY_UNITS))
      .build))

(defn enable-auto-scaling
  [scaling-client schema {:keys [ru] :or {ru 10}}]
  (let [table (common/get-fullname (:database schema) (:table schema))]
    (mapv (fn [type]
            (->> (make-scaling-request table ru type)
                 (.registerScalableTarget scaling-client)))
          [:read :write])))

(defn disable-auto-scaling
  [scaling-client schema]
  (let [table (common/get-fullname (:database schema) (:table schema))]
    (mapv (fn [type]
            (try
              (->> (make-scaling-disable-request table type)
                   (.deregisterScalableTarget scaling-client))
              (catch ObjectNotFoundException _
                (log/info "No auto-scaling configuration for " table))))
          [:read :write])))
