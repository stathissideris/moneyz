(ns moneyz.core
  (:use (incanter core charts io)))

#_(defn read-data
  "Use incanter's read-dataset instead."
  [filename]
  (let [reader (CSVReader. (get-input-reader filename) \, \" 0)
        data (doall (map seq (seq (.readAll reader))))
        columns (first data)
        rows (rest data)]
    (incanter.core.Dataset. (into [] columns) rows)))


(def d1
     (read-dataset
      "/Users/sideris/devel/moneyz/data/2011-08-31-statement export.csv"
      :header true))

(def d2
     (read-dataset
      "/Users/sideris/devel/moneyz/data/2011-09-05-statement export.csv"
      :header true))


(def d (group-by-account (merge-statements d1 d2)))

;;(view (time-series-plot (process-dates ($ :Date d)) ($ :Amount d)))