(ns dynamodemo.demo
  (:require [taoensso.faraday :as far]
            [clojure.java.io :as io]
            [clojure.tools.reader.edn :as edn]))

;; Setup amazon credentials -- these keys can be managed in the AWS console.
(def my-creds {:access-key ""
               :secret-key ""})

;; Insert an item
(far/put-item my-creds :my-table {:phonenum "512-555-1212" :firstName "Bob" :lastName "Smith" :addr "101 Main St"})

;; Retrieve an item
(far/get-item my-creds :my-table {:phonenum "512-555-1212"} )

;;
;; Demonstrate a batch put
;;

;; a small function to read my edn file of data into ram
(defn read-edn-from-file [filename]
  (with-open [rdr (io/reader filename)]
    (doall (map #(edn/read-string %) (line-seq rdr)))))

;; a small function to write a batch of data to my dynamodb table
(defn write-batch [batch]
  (far/batch-write-item my-creds  { :my-table { :put batch }}  {:span-reqs {:max 25 :throttle-ms 50}} ))

(defn load-to-dynamo [mylist]
  (let [tmplist (partition 25 25 nil mylist)
        plist   (vec tmplist)]
    (doseq [batch plist]
      (write-batch batch))))

;; read the data from disk
(def mylist (read-edn-from-file "reverse-lookup.txt"))

;; write the data to dynamodb
(load-to-dynamo mylist)
