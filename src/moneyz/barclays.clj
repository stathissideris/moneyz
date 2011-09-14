(ns moneyz.barclays
  (:use (incanter core charts io))
  (:require [clojure.set :as set])
  (:import (java.text SimpleDateFormat)))

(defn parse-date [date] (.parse (SimpleDateFormat. "dd/MM/yyyy") date))
(defn parse-dates [dates] (map parse-date dates))
(defn to-millis [dates] (map #(.getTime %) dates))
(def process-dates (comp to-millis parse-dates))

(defn get-csvs [path]
  (filter #(.endsWith % ".csv")
          (map #(.getAbsolutePath %)
               (.listFiles (java.io.File. path)))))

(defn merge-statements
  [d1 d2]
  (dataset
   (:column-names d1)
   (sort-by :Date (distinct (concat (:rows d1) (:rows d2))))))

(defn split-by-account [statement]
  (into {}
        (map
         (fn [[key d]] [key (dataset (:column-names statement) (sort-by :Date d))])
         (group-by :Account (:rows statement)))))

(defn calculate-balance-column [statement]
  (reductions + (map :Amount (:rows statement))))

(defn add-balance-column [statement]
  (let [balance-column (calculate-balance-column statement)]
    (-> statement
        (assoc :column-names (conj (:column-names statement) :Balance))
        (assoc :rows
          (into [] (map
                    (fn [row bal] (assoc row :Balance bal))
                    (:rows statement) balance-column))))))

(defn read-statement [filename]
  (read-dataset filename :header true))

(defn import-statements [data-path & [aliases]]
  (let [r (split-by-account
           (reduce merge-statements
                   (map read-statement
                        (get-csvs data-path))))
        r (zipmap (keys r) (map add-balance-column (vals r)))]
    (if aliases
      (set/rename-keys r aliases)
      r)))
