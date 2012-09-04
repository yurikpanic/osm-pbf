;;; -*- Lisp -*-
(defpackage com.elsyton.osm-pbf-system
  (:use :cl :asdf))

(in-package com.elsyton.osm-pbf-system)

(defsystem osm-pbf
  :description "Reading of OSM PBF format"
  :version "0.1"
  :author "Yuri Vishnevsky <vishnevsky@gmail.com>"
  :defsystem-depends-on (protobuf)
  :depends-on (:zlib)
  :components ((:protobuf-source-file "osmformat")
               (:protobuf-source-file "fileformat")
               (:file "b-tree")
               (:file "in-mem-str")
               (:file "osm-writer" :depends-on ("osmformat" "fileformat" "b-tree" "in-mem-str"))
               (:file "osm-reader" :depends-on ("osmformat" "fileformat" "b-tree" "osm-writer" "in-mem-str"))))
