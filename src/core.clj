(ns s3-sql.core
  (:gen-class)
  (require [aws.sdk.s3 :as s3])
  (require [clojure.java.io :as io])
  (require [clojure.contrib.math :as math])
  (require [postal.core :as mail])
  (use alex_and_georges.debug-repl))

(def not-nil? (complement nil?))

(defn send-status-message [backup-result]
  (println "Attempting to send e-mail with results: " backup-result)
  (println "Subject says: " (str "S3 Backup results for " (str (java.util.Date.))))
  (mail/send-message ^{:host "smtp.sendgrid.net"
                     :port 587
                     :user "sendgrid_user"
                     :pass "sendgrid_pass}
                   {:from "from_email_address"
                    :to ["to_email_address"]
                    :subject (str "S3 DFS Backup results for " (str (java.util.Date.)))
                    :body [{:type "text/html"
                            :content backup-result}]}))

(def cred
  {:access-key "access_key",
   :secret-key "secret_key})

; stolen from stackoverflow due to clojure not wanting to do recursion from within try/catch block
(defn try-times*
  "Executes thunk. If an exception is thrown, will retry. At most n retries
  are done. If still some exception is thrown it is bubbled upwards in
  the call chain."
  [n backup-name thunk]
  (loop [n n]
    (if-let [result (try
                      [(thunk)]
                      (catch Exception e
                        (when (zero? n)
                          (do
                            (str "Backup " backup-name " failed with exception: " e "!")))))]
      (comment (result 0))
      (recur (dec n)))))

(defmacro try-times
  "Executes body. If an exception is thrown, will retry. At most n retries
  are done. If still some exception is thrown it is bubbled upwards in
  the call chain."
  [n backup-name & body]
  `(try-times* ~n ~backup-name (fn [] ~@body)))

(def bytes-in-megabyte 1048576.0)

(defn upload-files [file-path]
   ; Determine part size based off of file part-size
   ; If total size < 5MB then use default size of 5MB
   (let [filesize (-> file-path
                      .length
                      (/ bytes-in-megabyte))]


     ; Determine part size based on file size
     (let [partsize
           (math/ceil (* (cond
                            (<= filesize 100) 5
                            (< filesize 1000) filesize
                            (< filesize 10000) (/ filesize 100.0)
                            (< filesize 100000) (/ filesize 1000.0)
                            :else (/ filesize 1050.0)) bytes-in-megabyte))
           file-name (.getName file-path)]

       ;(println "Using part size of: " (/ partsize bytes-in-megabyte))

       (println "Checking if " file-path " is a directory")

       (if (.isDirectory file-path)
         (do
           (println "getParent: " (.getParent file-path))
           (println "re-find: [" (get (re-find #"d:\\customerdata_backup\\(.+)" (.getPath file-path)) 1) "]")
           (let [directory (get (re-find #"d:\\customerdata_backup\\(.+)" (.getPath file-path)) 1)]
             (if (not-nil? directory)
               (do
                 (let [s3-bucket (clojure.string/replace (str "bucket-path-here" directory) "\\" "/")]
                   (println (str "Attempting to create directory: " s3-bucket))
                   (try-times 3 file-name
                              (let [result (s3/put-object cred s3-bucket "" "")]
                                (println (str "Result of creating directory: " result))))))))))
         (do
           ; Upload file to Amazon S3 and get result back
           (println file-path " is not a directory")
           (try-times 3 file-name
                      (let [backup-bucket (clojure.string/replace (get (re-find #"d:\\customerdata_backup\\(.+)"
                                                        (.getParent file-path)) 1) "\\" "/")
                            result (s3/put-multipart-object cred (str "bucket-path-here" backup-bucket)
                                                            file-name file-path {:part-size partsize})]
               (println (str "Using backup bucket: " backup-bucket))
               (str "Backup <B>" file-name "</B> with file size " (format "%.2f" filesize) "MB was successful!
               <B>ETag</B> is " (.getETag result) "<BR>")))))))

(defn -main []
  (let
    [file-path "d:\\customerdata_backup\\"
    fs (file-seq (io/file file-path))
    backups (filter #(re-matches #".*" (.getName %)) fs)]
    (do
     (let [upload-results (doall (map upload-files backups))]
      (send-status-message (apply str upload-results))))))
