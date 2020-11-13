(ns scalar-schema.dynamo.auto-scaling
  (:require [clojure.tools.logging :as log]
            [scalar-schema.dynamo.common :as dynamo])
  (:import (software.amazon.awssdk.regions Region)
           (software.amazon.awssdk.services.applicationautoscaling
            ApplicationAutoScalingClient)
           (software.amazon.awssdk.services.applicationautoscaling.model
            DeleteScalingPolicyRequest
            DeregisterScalableTargetRequest
            MetricType
            RegisterScalableTargetRequest
            ObjectNotFoundException
            PolicyType
            PredefinedMetricSpecification
            PutScalingPolicyRequest
            ScalableDimension
            ServiceNamespace
            TargetTrackingScalingPolicyConfiguration)))

;; TODO: options?
(def ^:const ^:private COOL_TIME_SEC (int 60))
(def ^:const ^:private ^double TARGET_USAGE_RATE 70.0)

(def ^:private SCALABLE_DIMENSIONS
  {:read ScalableDimension/DYNAMODB_TABLE_READ_CAPACITY_UNITS
   :write ScalableDimension/DYNAMODB_TABLE_WRITE_CAPACITY_UNITS})

(defn get-scaling-client
  [user password region]
  (-> (ApplicationAutoScalingClient/builder)
      (.credentialsProvider (dynamo/get-credentials-provider user password))
      (.region (Region/of region))
      .build))

(defn- make-scaling-request
  [schema ru type]
  (-> (RegisterScalableTargetRequest/builder)
      (.serviceNamespace ServiceNamespace/DYNAMODB)
      (.resourceId (str "table/" (dynamo/get-table-name schema)))
      (.scalableDimension (get SCALABLE_DIMENSIONS type))
      (.minCapacity (int (if (> ru 10) (/ ru 10) ru)))
      (.maxCapacity ru)
      .build))

(defn- make-scaling-disable-request
  [schema type]
  (-> (DeregisterScalableTargetRequest/builder)
      (.serviceNamespace ServiceNamespace/DYNAMODB)
      (.resourceId (str "table/" (dynamo/get-table-name schema)))
      (.scalableDimension (get SCALABLE_DIMENSIONS type))
      .build))

(defn- make-scaling-policy-config
  [type]
  (-> (TargetTrackingScalingPolicyConfiguration/builder)
      (.predefinedMetricSpecification
       (-> (PredefinedMetricSpecification/builder)
           (.predefinedMetricType
            (if (= type :read)
              MetricType/DYNAMO_DB_READ_CAPACITY_UTILIZATION
              MetricType/DYNAMO_DB_WRITE_CAPACITY_UTILIZATION))
           .build))
      (.scaleInCooldown COOL_TIME_SEC)
      (.scaleOutCooldown COOL_TIME_SEC)
      (.targetValue TARGET_USAGE_RATE)
      .build))

(defn- make-scaling-policy-request
  [schema type]
  (let [table (dynamo/get-table-name schema)]
    (-> (PutScalingPolicyRequest/builder)
        (.serviceNamespace ServiceNamespace/DYNAMODB)
        (.resourceId (str "table/" table))
        (.scalableDimension (get SCALABLE_DIMENSIONS type))
        (.policyName (str table \- (name type)))
        (.policyType PolicyType/TARGET_TRACKING_SCALING)
        (.targetTrackingScalingPolicyConfiguration
         (make-scaling-policy-config type))
        .build)))

(defn- make-scaling-policy-delete-request
  [schema type]
  (let [table (dynamo/get-table-name schema)]
    (-> (DeleteScalingPolicyRequest/builder)
        (.serviceNamespace ServiceNamespace/DYNAMODB)
        (.resourceId (str "table/" table))
        (.scalableDimension (get SCALABLE_DIMENSIONS type))
        (.policyName (str table \- (name type)))
        .build)))

(defn enable-auto-scaling
  [scaling-client schema {:keys [ru] :or {ru 10}}]
  (mapv (fn [type]
          (doto scaling-client
            (.registerScalableTarget (make-scaling-request schema ru type))
            (.putScalingPolicy (make-scaling-policy-request schema type))))
        [:read :write]))

(defn disable-auto-scaling
  [scaling-client schema]
  (mapv (fn [type]
          (try
            (doto scaling-client
              (.deregisterScalableTarget
               (make-scaling-disable-request schema type))
              (.deleteScalingPolicy
               (make-scaling-policy-delete-request schema type)))
            (catch ObjectNotFoundException _
              (log/info "No auto-scaling configuration for"
                        (dynamo/get-table-name schema)))))
        [:read :write]))
