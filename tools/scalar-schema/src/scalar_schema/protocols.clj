(ns scalar-schema.protocols)

(defprotocol IOperator
  (create-table [this schema opts])
  (delete-table [this schema opts])
  (close [this opts]))
