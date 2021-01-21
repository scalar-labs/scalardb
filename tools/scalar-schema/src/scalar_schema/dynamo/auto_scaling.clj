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
   :write ScalableDimension/DYNAMODB_TABLE_WRITE_CAPACITY_UNITS
   :index-read ScalableDimension/DYNAMODB_INDEX_READ_CAPACITY_UNITS
   :index-write ScalableDimension/DYNAMODB_INDEX_WRITE_CAPACITY_UNITS})

(defn get-scaling-client
  [user password region]
  (-> (ApplicationAutoScalingClient/builder)
      (.credentialsProvider (dynamo/get-credentials-provider user password))
      (.region (Region/of region))
      .build))

(defn- get-resource-id
  [table index-table]
  (str "table/" table (when index-table (str "/index/" index-table))))

(defn- get-policy-name
  [table index-table type]
  (str table \- (when index-table (str index-table \-)) (name type)))

(defn- scaling-request-fn
  [ru]
  (fn [table index-table type]
    (-> (RegisterScalableTargetRequest/builder)
        (.serviceNamespace ServiceNamespace/DYNAMODB)
        (.resourceId (get-resource-id table index-table))
        (.scalableDimension (get SCALABLE_DIMENSIONS type))
        (.minCapacity (int (if (> ru 10) (/ ru 10) ru)))
        (.maxCapacity ru)
        .build)))

(defn- scaling-disable-request-fn
  []
  (fn [table index-table type]
    (-> (DeregisterScalableTargetRequest/builder)
        (.serviceNamespace ServiceNamespace/DYNAMODB)
        (.resourceId (get-resource-id table index-table))
        (.scalableDimension (get SCALABLE_DIMENSIONS type))
        .build)))

(defn- make-scaling-policy-config
  [type]
  (-> (TargetTrackingScalingPolicyConfiguration/builder)
      (.predefinedMetricSpecification
       (-> (PredefinedMetricSpecification/builder)
           (.predefinedMetricType
            (if (or (= type :read) (= type :index-read))
              MetricType/DYNAMO_DB_READ_CAPACITY_UTILIZATION
              MetricType/DYNAMO_DB_WRITE_CAPACITY_UTILIZATION))
           .build))
      (.scaleInCooldown COOL_TIME_SEC)
      (.scaleOutCooldown COOL_TIME_SEC)
      (.targetValue TARGET_USAGE_RATE)
      .build))

(defn- scaling-policy-request-fn
  []
  (fn [table index-table type]
    (-> (PutScalingPolicyRequest/builder)
        (.serviceNamespace ServiceNamespace/DYNAMODB)
        (.resourceId (get-resource-id table index-table))
        (.scalableDimension (get SCALABLE_DIMENSIONS type))
        (.policyName (get-policy-name table index-table type))
        (.policyType PolicyType/TARGET_TRACKING_SCALING)
        (.targetTrackingScalingPolicyConfiguration
         (make-scaling-policy-config type))
        .build)))

(defn- scaling-policy-delete-request-fn
  []
  (fn
    [table index-table type]
    (-> (DeleteScalingPolicyRequest/builder)
        (.serviceNamespace ServiceNamespace/DYNAMODB)
        (.resourceId (get-resource-id table index-table))
        (.scalableDimension (get SCALABLE_DIMENSIONS type))
        (.policyName (get-policy-name table index-table type))
        .build)))

(defn- make-requests
  [f table global-index-tables]
  (-> (map #(f table nil %) [:read :write])
      (into (map #(f table % :index-read) global-index-tables))
      (into (map #(f table % :index-write) global-index-tables))))

(defn enable-auto-scaling
  [scaling-client schema {:keys [ru] :or {ru (int 10)}}]
  (let [table (dynamo/get-table-name schema)
        global-index-tables (map #(dynamo/get-global-index-name schema %)
                                 (:secondary-index schema))
        scaling-reqs (make-requests (scaling-request-fn ru)
                                    table
                                    global-index-tables)
        policy-reqs (make-requests (scaling-policy-request-fn)
                                   table
                                   global-index-tables)]
    (mapv (fn [scaling-req policy-req]
            (doto scaling-client
              (.registerScalableTarget scaling-req)
              (.putScalingPolicy policy-req)))
          scaling-reqs policy-reqs)))

(defn disable-auto-scaling
  [scaling-client schema]
  (let [table (dynamo/get-table-name schema)
        global-index-tables (map #(dynamo/get-global-index-name schema %)
                                 (:secondary-index schema))
        scaling-reqs (make-requests (scaling-disable-request-fn)
                                    table
                                    global-index-tables)
        policy-reqs (make-requests (scaling-policy-delete-request-fn)
                                   table
                                   global-index-tables)]
    (mapv (fn [scaling-req policy-req]
            (try
              (doto scaling-client
                (.deregisterScalableTarget scaling-req)
                (.deleteScalingPolicy policy-req))
              (catch ObjectNotFoundException _
                (log/info "No auto-scaling configuration for"
                          (dynamo/get-table-name schema)))))
          scaling-reqs policy-reqs)))
