;;; -*- Lisp -*-
(defpackage com.elsyton.osm-pbf-system
  (:use :cl :asdf))

(in-package com.elsyton.osm-pbf-system)

(defsystem osm-pbf
  :description "Reading of OSM PBF format"
  :version "0.1"
  :author "Yuri Vishnevsky <vishnevsky@gmail.com>"
  :defsystem-depends-on (protobuf zlib)
  :depends-on (:zlib)
  :components ((:protobuf-source-file "osmformat")
               (:protobuf-source-file "fileformat")
               (:protobuf-source-file "btree")
               (:protobuf-source-file "osmbtree")
               (:protobuf-source-file "bbox")
               (:file "b-tree" :depends-on ("btree"))
               (:file "in-mem-str" :depends-on ("osmbtree"))
               (:file "osm-index-search" :depends-on ("osmformat" "fileformat" "btree" "osmbtree"))
               (:file "osm-writer" :depends-on ("osmformat" "fileformat" "b-tree" "in-mem-str"))
               (:file "osm-reader" :depends-on ("osmformat" "fileformat" "b-tree" "osm-writer" "in-mem-str"))))
