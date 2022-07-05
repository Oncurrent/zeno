(ns com.oncurrent.zeno.state-providers.crdt.server-crdt
  (:require
   [com.oncurrent.zeno.utils :as u]
   [deercreeklabs.lancaster :as l]
   [deercreeklabs.lancaster.fingerprint :as lfp]
   [taoensso.timbre :as log])
  (:import
   (java.nio ByteBuffer)
   (java.util Arrays)
   (jdk.incubator.foreign MemorySegment
                          ValueLayout)
   (jdk.incubator.vector Vector)))

(set! *warn-on-reflection* true)

;;;;;;;;;;;;;;;;;;;;;;;; Server CRDT Op Byte Array Layout ;;;;;;;;;;;;;;;;;;;;
;;
;; 128 Bytes long (2 cache lines)
;;
;; 0                   1                   2                   3
;; 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
;; +---------------+---------------+---------------+---------------+
;; |               |               |               |               |
;; | Path          | Path          | Path          | Path          |
;; | Element 0     | Element 1     | Element 2     | Element 3     |
;; | (8)           | (8)           | (8)           | (8)           |
;; +---------------+---------------+---------------+---------------+
;; |               |               |               |               |
;; | Path          | Path          | Path          | Path          |
;; | Element 4     | Element 5     | Element 6     | Element 7     |
;; | (8)           | (8)           | (8)           | (8)           |
;; +---------------+---------------+---------------+---------------+
;; |               |               |               |               |
;; | Path          | Path          | Path          | Path          |
;; | Element 8     | Element 9     | Element 10    | Element 11    |
;; | (8)           | (8)           | (8)           | (8)           |
;; +-+-+-+-+-------+-------+-------+---------------+---------------+
;; |P|O|V|R| Sys   | Add   | Add   |                               |
;; |L|T|T|S| Time  | ID    | ID    | Value                         |
;; |E|Y|Y|V| Secs  | Client| Seq   | (16)                          |
;; |N|P|P| | (4)   | (4)   | (4)   |                               |
;; +-+-+-+-+-------+-------+-------+-------------------------------+
;;
;;  PLEN = Path Length
;;  OTYP = Op Type
;;  VTYP = Value Type
;;  RSV  = Reserved / Padding
;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def op-byte-array-len 128)
(def path-element-len 8)
(def path-len-offset 96)

;; WARNING: Only add to the bottom of this list; do not insert in the
;; middle. Doing so will change the meaning of stored data.
(def op-types [:add-array-edge
               :add-container
               :add-value
               :delete-array-edge
               :delete-container
               :delete-value])

(def keyword-marker "K")
(def string-marker "S")

(defn insert-ba [ba index item]
  (let [len (count ba)
        new-ba ^bytes (byte-array (inc len))
        old-mem-seg ^MemorySegment (MemorySegment/ofArray ^bytes ba)
        new-mem-seg ^MemorySegment (MemorySegment/ofArray ^bytes new-ba)]
    (MemorySegment/copy old-mem-seg 0
                        new-mem-seg 0
                        (long index))
    (.set new-mem-seg ValueLayout/JAVA_BYTE (long index) ^byte item)
    (MemorySegment/copy old-mem-seg (long index)
                        new-mem-seg (long (inc index))
                        (long (- len index)))
    new-ba))
#_
(defn read-path [^MemorySegment mem-seg]
  (let [path-len (.get mem-seg ValueLayout/JAVA_BYTE (long path-len-offset))
        last-i (dec path-len)]
    (if (zero? path-len)
      []
      (loop [i 0]
        (let [hash (.get mem-seg
                         ValueLayout/JAVA_LONG
                         (long (* i path-element-len)))
              s (hash->string)]
          (dslfsj))))))

(defn write-path! [path ^MemorySegment mem-seg]
  (let [path-len (count path)
        last-i (dec path-len)]
    (when (pos? path-len)
      (loop [i 0]
        (let [el (nth path i)
              s (cond
                  (keyword? el)
                  (str keyword-marker el)

                  (string? el)
                  (str string-marker el)

                  :else
                  (throw (ex-info
                          (str "Bad path element `" (or el "nil") "`.")
                          (u/sym-map path el))))]
          (.set mem-seg
                ValueLayout/JAVA_LONG
                (long (* i path-element-len))
                (long (lfp/fingerprint64 s)))
          (when (< i last-i)
            (recur (inc i)))))
      (.set mem-seg
            ValueLayout/JAVA_BYTE
            (long path-len-offset)
            (byte path-len)))))

(defn op-map->op-byte-array [op-map]
  (let [ba (byte-array op-byte-array-len)
        mem-seg ^MemorySegment (MemorySegment/ofArray ^bytes ba)]
    (write-path! (vec (:path op-map)) mem-seg)
    ba))
