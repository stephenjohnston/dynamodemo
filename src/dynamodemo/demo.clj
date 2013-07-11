(ns dynamodemo.demo
  (:require [taoensso.faraday :as far]
            [clojure.java.io :as io]
            [clojure.tools.reader.edn :as edn]))

(def my-creds {:access-key ""
               :secret-key ""})

;; Insert an item
(far/put-item my-creds :my-table {:phonenum "512-555-1212" :firstName "Bob" :lastName "Smith" :addr "104 Main St" :zip "78750" })

;; Retrieve an item
(far/get-item my-creds :my-table {:phonenum "512-555-1212"} )

;; Demonstrate a batch put

;; a small function to read my edn file of data into ram
(defn read-edn-from-file [filename]
  (with-open [rdr (io/reader filename)]
    (doall (map #(edn/read-string %) (line-seq rdr)))))

;; a small function to write a batch of data to my dynamodb table
(defn write-batch [batch tablename]
  (far/batch-write-item my-creds  { tablename { :put batch }}  {:span-reqs {:max 25 :throttle-ms 50}} ))

(defn load-to-dynamo [mylist tablename]
  (let [batch-list (partition 25 25 nil mylist)]
    (doseq [batch batch-list]
      (write-batch batch tablename))))

;; read the data from disk
(def mylist (read-edn-from-file (io/resource "reverse-lookup.edn")))

;; write the reverse lookup data to dynamodb
(load-to-dynamo mylist :my-table)

;; read the data from disk
(def metadata (read-edn-from-file (io/resource "phone-metadata.edn")))

;; write the metadata to dynamodb
(load-to-dynamo metadata :phone-metadata)
;; Hash + Range examples

;; get retrieves at most, 1 row
(far/get-item my-creds :phone-metadata {:phonenum "512-764-1552" :calltime 1372036212494})

;; batch-get retrieves at most, 100 rows (one to one with the number of keys requested)
;; Queries can only be used with hash+range tables, and a single query may return many rows

;; query a dynamodb table
(far/query my-creds :phone-metadata {:phonenum [ :EQ ["512-764-1552"]] :calltime [ :GT [1372036212490]] })

;; using a local secondary index
(far/query my-creds :phone-metadata {:phonenum [ :EQ ["512-764-1552"]] :duration [ :GE [26]] } { :index "duration-index"})
