(ns moneyz.core
  (:use (incanter core charts io))
  (:import (java.text SimpleDateFormat)))

#_(defn read-data
  "Use incanter's read-dataset instead."
  [filename]
  (let [reader (CSVReader. (get-input-reader filename) \, \" 0)
        data (doall (map seq (seq (.readAll reader))))
        columns (first data)
        rows (rest data)]
    (incanter.core.Dataset. (into [] columns) rows)))

(defn parse-date [date] (.parse (SimpleDateFormat. "dd/MM/yyyy") date))
(defn parse-dates [dates] (map parse-date dates))
(defn to-millis [dates] (map #(.getTime %) dates))
(def process-dates (comp to-millis parse-dates))

(def d1
     (read-dataset
      "/Users/sideris/devel/moneyz/data/2011-08-31-statement export.csv"
      :header true))

(def d2
     (read-dataset
      "/Users/sideris/devel/moneyz/data/2011-09-05-statement export.csv"
      :header true))

(defn merge-statements
  [d1 d2]
  (dataset
   (:column-names d1)
   (sort-by :Date (distinct (concat (:rows d1) (:rows d2))))))

(defn group-by-account [statement]
  (into {}
        (map
         (fn [[key d]] [key (dataset (:column-names statement) d)])
         (group-by :Account (:rows statement)))))

(defn calculate-balance-column [statement]
  (reverse (reductions + (reverse (map :Amount (:rows statement))))))

(defn add-balance-column [statement]
  (let [balance-column (calculate-balance-column statement)]
    (-> statement
        (assoc :column-names (conj (:column-names statement) :Balance))
        (assoc :rows
          (into [] (map
                    (fn [row bal] (assoc row :Balance bal))
                    (:rows statement) balance-column))))))

(def d (group-by-account (merge-statements d1 d2)))

;;(view (time-series-plot (process-dates ($ :Date d)) ($ :Amount d)))