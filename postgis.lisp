(defpackage :osm-postgis
  (:nicknames :db)
  (:use :cl
        :postmodern
        :s-sql
        :cl-postgres
        :b-tree
        :in-mem-str)
  (:export :default-connect
           :write-way))

(in-package :osm-postgis)

(defvar *db-host* "localhost")
(defvar *db-name* "gis")
(defvar *db-user* "gis")
(defvar *db-password* "gis")

(defun default-connect ()
  (when *database* (disconnect-toplevel))
  (connect-toplevel *db-name* *db-user* *db-password* *db-host*))

(defun make-way-linestring (way nodes-btree)
  (let ((points-list
         (loop for nid across (way-refs way)
            collect (let ((node (bsearch nodes-btree nid)))
                      (unless node (return-from make-way-linestring nil))
                      ;; FIXME! hardcoded offsets and granularity 
                      (format nil "~F ~F"
                              (* .000000001 100 (node-lon node))
                              (* .000000001 100 (node-lat node)))))))
    (format nil "SRID=4326;LINESTRING(~A~{,~A~})" (car points-list) (cdr points-list))))

(defun write-way (way nodes-btree)
  (let ((ls (make-way-linestring way nodes-btree)))
    (when ls
      (execute
       (format nil "insert into way values (~D, st_geomfromewkt(~A))" (way-id way) (sql-escape ls)))
      t)))
