(ns unit.crdt-commands-test
  (:require
   [clojure.set :as set]
   [clojure.test :as t :refer [deftest is]]
   [deercreeklabs.lancaster :as l]
   [com.oncurrent.zeno.crdt.commands :as commands]
   [com.oncurrent.zeno.crdt :as crdt]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log])
  #?(:clj
     (:import
      (clojure.lang ExceptionInfo))))

(l/def-record-schema pet-schema
  [:name l/string-schema]
  [:species l/string-schema])

(l/def-record-schema pet-owner-schema
  [:name l/string-schema]
  [:pets (l/array-schema pet-schema)])

(deftest test-crdt-set
  (let [sys-time-ms (u/str->long "1643061294782")
        arg {:cmds [{:zeno/arg "Hi"
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt]}]
             :crdt-schema l/string-schema
             :sys-time-ms sys-time-ms}
        {:keys [crdt ops]} (commands/process-cmds arg)
        expected-value "Hi"]
    (is (= expected-value (crdt/get-value {:crdt crdt
                                           :path []
                                           :schema (:crdt-schema arg)})))))

(deftest test-crdt-set-and-remove
  (let [sys-time-ms (u/str->long "1643061294782")
        arg {:cmds [{:zeno/arg "Hello"
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt]}
                    {:zeno/op :zeno/remove
                     :zeno/path [:zeno/crdt]}]
             :crdt-schema l/string-schema
             :sys-time-ms sys-time-ms}
        {:keys [crdt ops]} (commands/process-cmds arg)
        expected-value nil]
    (is (= expected-value (crdt/get-value {:crdt crdt
                                           :path []
                                           :schema (:crdt-schema arg)})))))

(deftest test-crdt-set-and-reset
  (let [sys-time-ms (u/str->long "1643061294782")
        arg {:cmds [{:zeno/arg "Hello"
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt]}
                    {:zeno/arg "Goodbye"
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt]}]
             :crdt-schema l/string-schema
             :sys-time-ms sys-time-ms}
        {:keys [crdt ops]} (commands/process-cmds arg)
        expected-value "Goodbye"]
    (is (= expected-value (crdt/get-value {:crdt crdt
                                           :path []
                                           :schema (:crdt-schema arg)})))))

(deftest test-crdt-map-set-and-reset
  (let [sys-time-ms (u/str->long "1643061294782")
        arg {:cmds [{:zeno/arg 31
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt "Alice"]}
                    {:zeno/arg 8
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt "Bob"]}
                    {:zeno/arg 12
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt "Bob"]}]
             :crdt-schema (l/map-schema l/int-schema)
             :sys-time-ms sys-time-ms}
        {:keys [crdt ops]} (commands/process-cmds arg)
        expected-value {"Alice" 31
                        "Bob" 12}]
    (is (= expected-value (crdt/get-value {:crdt crdt
                                           :path []
                                           :schema (:crdt-schema arg)})))))

(deftest test-crdt-record-set-and-reset
  (let [sys-time-ms (u/str->long "1643061294782")
        arg {:cmds [{:zeno/arg "Lamby"
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt :name]}
                    {:zeno/arg "Ovis aries"
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt :species]}
                    {:zeno/arg "Sheepy"
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt :name]}]
             :crdt-schema pet-schema
             :sys-time-ms sys-time-ms}
        {:keys [crdt ops]} (commands/process-cmds arg)
        expected-value {:name "Sheepy"
                        :species "Ovis aries"}]
    (is (= expected-value (crdt/get-value {:crdt crdt
                                           :path []
                                           :schema (:crdt-schema arg)})))))

(deftest test-crdt-union-set-and-reset
  (let [sys-time-ms (u/str->long "1643061294782")
        arg {:cmds [{:zeno/arg 3.14
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt]}
                    {:zeno/arg "pi"
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt]}]
             :crdt-schema (l/union-schema [l/string-schema l/float-schema])
             :sys-time-ms sys-time-ms}
        {:keys [crdt ops]} (commands/process-cmds arg)
        expected-value "pi"]
    (is (= expected-value (crdt/get-value {:crdt crdt
                                           :path []
                                           :schema (:crdt-schema arg)})))))

(deftest test-crdt-array-set
  (let [sys-time-ms (u/str->long "1643061294999")
        arg {:cmds [{:zeno/arg ["Hi" "There"]
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt]}]
             :crdt-schema (l/array-schema l/string-schema)
             :sys-time-ms sys-time-ms}
        {:keys [crdt ops]} (commands/process-cmds arg)
        expected-value ["Hi" "There"]]
    (is (= expected-value (crdt/get-value {:crdt crdt
                                           :path []
                                           :schema (:crdt-schema arg)})))))

(deftest test-crdt-array-set-and-remove
  (let [sys-time-ms (u/str->long "1643061294999")
        arg {:cmds [{:zeno/arg ["Hi" "There"]
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt]}
                    {:zeno/op :zeno/remove
                     :zeno/path [:zeno/crdt 0]}]
             :crdt-schema (l/array-schema l/string-schema)
             :sys-time-ms sys-time-ms}
        {:keys [crdt ops]} (commands/process-cmds arg)
        expected-value ["There"]]
    (is (= expected-value (crdt/get-value {:crdt crdt
                                           :path []
                                           :schema (:crdt-schema arg)})))))

(deftest test-crdt-array-of-maps-set-and-remove
  (let [sys-time-ms (u/str->long "1643061294999")
        arg {:cmds [{:zeno/arg [{"a" 1 "b" 2}
                                {"y" 10 "z" 20}
                                {"c" 3}]
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt]}
                    {:zeno/op :zeno/remove
                     :zeno/path [:zeno/crdt 1 "y"]}]
             :crdt-schema (l/array-schema (l/map-schema l/int-schema))
             :sys-time-ms sys-time-ms}
        {:keys [crdt ops]} (commands/process-cmds arg)
        expected-value [{"a" 1 "b" 2}
                        {"z" 20}
                        {"c" 3}]]
    (is (= expected-value (crdt/get-value {:crdt crdt
                                           :path []
                                           :schema (:crdt-schema arg)})))))

(deftest test-crdt-array-set-and-reset
  (let [arg {:cmds [{:zeno/arg ["Hi" "There"]
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt]}
                    {:zeno/arg "Go"
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt 0]}]
             :crdt-schema (l/array-schema l/string-schema)}
        {:keys [crdt ops]} (commands/process-cmds arg)
        expected-value ["Go" "There"]]
    (is (= expected-value (crdt/get-value {:crdt crdt
                                           :path []
                                           :schema (:crdt-schema arg)})))))

(deftest test-crdt-array-simple-inserts
  (let [arg {:cmds [{:zeno/arg ["Hi" "There"]
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt]}
                    {:zeno/arg "Hello!"
                     :zeno/op :zeno/insert-before
                     :zeno/path [:zeno/crdt 0]}
                    {:zeno/arg "Bob"
                     :zeno/op :zeno/insert-after
                     :zeno/path [:zeno/crdt -1]}]
             :crdt-schema (l/array-schema l/string-schema)}
        {:keys [crdt ops]} (commands/process-cmds arg)
        expected-value ["Hello!" "Hi" "There" "Bob"]]
    (is (= expected-value (crdt/get-value {:crdt crdt
                                           :path []
                                           :schema (:crdt-schema arg)})))))

(deftest test-crdt-array-insert-before-into-empty
  (let [arg {:cmds [{:zeno/arg "Hello!"
                     :zeno/op :zeno/insert-before
                     :zeno/path [:zeno/crdt 0]}]
             :crdt-schema (l/array-schema l/string-schema)}
        {:keys [crdt ops]} (commands/process-cmds arg)
        expected-value ["Hello!"]]
    (is (= expected-value (crdt/get-value {:crdt crdt
                                           :path []
                                           :schema (:crdt-schema arg)})))))

(deftest test-crdt-array-insert-after-into-empty
  (let [sys-time-ms (u/str->long "1643061294999")
        arg {:cmds [{:zeno/arg "Hello!"
                     :zeno/op :zeno/insert-after
                     :zeno/path [:zeno/crdt -1]}]
             :crdt-schema (l/array-schema l/string-schema)
             :sys-time-ms sys-time-ms}
        {:keys [crdt ops]} (commands/process-cmds arg)
        ret (crdt/get-value-info {:crdt crdt
                                  :path [-1]
                                  :schema (:crdt-schema arg)})
        _ (is (= {:norm-path [0]
                  :value "Hello!"}
                 ret))
        expected-value ["Hello!"]
        value (crdt/get-value {:crdt crdt
                               :path []
                               :schema (:crdt-schema arg)})]
    (is (= expected-value value))))

(deftest ^:this test-crdt-array-insert-range-after-into-empty
  (let [sys-time-ms (u/str->long "1643061294999")
        arg {:cmds [{:zeno/arg ["1" "2" "3"]
                     :zeno/op :zeno/insert-range-after
                     :zeno/path [:zeno/crdt -1]}]
             :crdt-schema (l/array-schema l/string-schema)
             :sys-time-ms sys-time-ms}
        {:keys [crdt ops]} (commands/process-cmds arg)
        expected-value ["1" "2" "3"]]
    (is (= expected-value (crdt/get-value {:crdt crdt
                                           :path []
                                           :schema (:crdt-schema arg)})))))

(deftest test-crdt-array-insert-range-before-into-empty
  (let [sys-time-ms (u/str->long "1643061294999")
        arg {:cmds [{:zeno/arg ["1" "2" "3"]
                     :zeno/op :zeno/insert-range-before
                     :zeno/path [:zeno/crdt 0]}]
             :crdt-schema (l/array-schema l/string-schema)
             :sys-time-ms sys-time-ms}
        {:keys [crdt ops]} (commands/process-cmds arg)
        expected-value ["1" "2" "3"]]
    (is (= expected-value (crdt/get-value {:crdt crdt
                                           :path []
                                           :schema (:crdt-schema arg)})))))

(deftest test-crdt-array-insert-range-before-into-front
  (let [arg {:cmds [{:zeno/arg ["4" "5"]
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt]}
                    {:zeno/arg ["1" "2" "3"]
                     :zeno/op :zeno/insert-range-before
                     :zeno/path [:zeno/crdt 0]}]
             :crdt-schema (l/array-schema l/string-schema)}
        {:keys [crdt ops]} (commands/process-cmds arg)
        expected-value ["1" "2" "3" "4" "5"]]
    (is (= expected-value (crdt/get-value {:crdt crdt
                                           :path []
                                           :schema (:crdt-schema arg)})))))

(deftest test-crdt-array-insert-range-after-end
  (let [arg {:cmds [{:zeno/arg ["4" "5"]
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt]}
                    {:zeno/arg ["1" "2" "3"]
                     :zeno/op :zeno/insert-range-after
                     :zeno/path [:zeno/crdt -1]}]
             :crdt-schema (l/array-schema l/string-schema)}
        {:keys [crdt ops]} (commands/process-cmds arg)
        expected-value ["4" "5" "1" "2" "3"]]
    (is (= expected-value (crdt/get-value {:crdt crdt
                                           :path []
                                           :schema (:crdt-schema arg)})))))

(deftest test-crdt-array-insert-range-before-into-middle
  (let [arg {:cmds [{:zeno/arg ["1" "2" "3"]
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt]}
                    {:zeno/arg ["A" "B"]
                     :zeno/op :zeno/insert-range-before
                     :zeno/path [:zeno/crdt -2]}]
             :crdt-schema (l/array-schema l/string-schema)}
        {:keys [crdt ops]} (commands/process-cmds arg)
        expected-value ["1" "A" "B" "2" "3"]]
    (is (= expected-value (crdt/get-value {:crdt crdt
                                           :path []
                                           :schema (:crdt-schema arg)})))))

(deftest test-crdt-array-insert-range-after-into-middle
  (let [arg {:cmds [{:zeno/arg ["1" "2" "3"]
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt]}
                    {:zeno/arg ["A" "B"]
                     :zeno/op :zeno/insert-range-after
                     :zeno/path [:zeno/crdt -2]}]
             :crdt-schema (l/array-schema l/string-schema)}
        {:keys [crdt]} (commands/process-cmds arg)
        expected-value ["1" "2" "A" "B" "3"]]
    (is (= expected-value (crdt/get-value {:crdt crdt
                                           :path []
                                           :schema (:crdt-schema arg)})))))

(deftest test-nested-set
  (let [arg {:cmds [{:zeno/arg {:name "Bill"
                                :pets [{:name "Pinky"
                                        :species "Felis catus"}
                                       {:name "Fishy"
                                        :species "Carassius auratus"}]}
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt]}
                    {:zeno/arg "Goldy"
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt :pets -1 :name]}]
             :crdt-schema pet-owner-schema}
        {:keys [crdt]} (commands/process-cmds arg)
        expected-value {:name "Bill"
                        :pets [{:name "Pinky"
                                :species "Felis catus"}
                               {:name "Goldy"
                                :species "Carassius auratus"}]}]
    (is (= expected-value (crdt/get-value {:crdt crdt
                                           :path []
                                           :schema (:crdt-schema arg)})))
    (is (= {:name "Pinky"
            :species "Felis catus"}
           (crdt/get-value {:crdt crdt
                            :path [:pets -2]
                            :schema (:crdt-schema arg)})))))

(deftest test-nested-set-and-remove
  (let [*next-id-num (atom 0)
        arg {:cmds [{:zeno/arg {:name "Bill"
                                :pets [{:name "Pinky"
                                        :species "Felis catus"}
                                       {:name "Fishy"
                                        :species "Carassius auratus"}]}
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt]}
                    {:zeno/op :zeno/remove
                     :zeno/path [:zeno/crdt :pets 0]}]
             :crdt-schema pet-owner-schema
             :make-id #(let [n (swap! *next-id-num inc)]
                         (str "I" n))}
        {:keys [crdt ops]} (commands/process-cmds arg)
        expected-value {:name "Bill"
                        :pets [{:name "Fishy"
                                :species "Carassius auratus"}]}]
    (is (= expected-value (crdt/get-value {:crdt crdt
                                           :path []
                                           :schema (:crdt-schema arg)})))))

(deftest test-subsequent-txns
  (let [arg1 {:cmds [{:zeno/arg {:name "Bill"
                                 :pets [{:name "Pinky"
                                         :species "Felis catus"}
                                        {:name "Fishy"
                                         :species "Carassius auratus"}]}
                      :zeno/op :zeno/set
                      :zeno/path [:zeno/crdt]}]
              :crdt-schema pet-owner-schema}
        ret1 (commands/process-cmds arg1)
        arg2 {:cmds [{:zeno/op :zeno/remove
                      :zeno/path [:zeno/crdt :pets 0]}]
              :crdt (:crdt ret1)
              :crdt-schema pet-owner-schema}
        ret2 (commands/process-cmds arg2)
        expected-value {:name "Bill"
                        :pets [{:name "Fishy"
                                :species "Carassius auratus"}]}]
    (is (= expected-value (crdt/get-value {:crdt (:crdt ret2)
                                           :path []
                                           :schema (:crdt-schema arg1)})))))

(deftest test-nested-merge-array-no-conflict
  (let [arg0 {:cmds [{:zeno/arg {:name "Bill"
                                 :pets [{:name "Pinky"
                                         :species "Felis catus"}
                                        {:name "Fishy"
                                         :species "Carassius auratus"}]}
                      :zeno/op :zeno/set
                      :zeno/path [:zeno/crdt]}]
              :crdt-schema pet-owner-schema}
        ret0 (commands/process-cmds arg0)
        arg1 {:cmds [{:zeno/arg {:name "Chris"
                                 :species "Canis familiaris"}
                      :zeno/op :zeno/insert-before
                      :zeno/path [:zeno/crdt :pets 0]}]
              :crdt (:crdt ret0)
              :crdt-schema pet-owner-schema}
        arg2 {:cmds [{:zeno/arg {:name "Pat"
                                 :species "Canis familiaris"}
                      :zeno/op :zeno/insert-after
                      :zeno/path [:zeno/crdt :pets -1]}]
              :crdt (:crdt ret0)
              :crdt-schema pet-owner-schema}
        ret1 (commands/process-cmds arg1)
        ret2 (commands/process-cmds arg2)
        new-ops (set/union (:ops ret1) (:ops ret2))
        merged-crdt (crdt/apply-ops {:crdt (:crdt ret0)
                                     :ops new-ops
                                     :schema pet-owner-schema})
        expected-value [{:name "Chris"
                         :species "Canis familiaris"}
                        {:name "Pinky"
                         :species "Felis catus"}
                        {:name "Fishy"
                         :species "Carassius auratus"}
                        {:name "Pat"
                         :species "Canis familiaris"}]]
    (is (= expected-value (crdt/get-value {:crdt merged-crdt
                                           :path [:pets]
                                           :schema pet-owner-schema})))))

(deftest test-nested-merge-array-conflict
  (let [*next-id-num (atom 0)
        make-id #(let [n (swap! *next-id-num inc)]
                   (str "I" n))
        arg0 {:cmds [{:zeno/arg {:name "Bill"
                                 :pets [{:name "Pinky"
                                         :species "Felis catus"}
                                        {:name "Fishy"
                                         :species "Carassius auratus"}]}
                      :zeno/op :zeno/set
                      :zeno/path [:zeno/crdt]}]
              :crdt-schema pet-owner-schema
              :make-id make-id}
        ret0 (commands/process-cmds arg0)
        arg1 {:cmds [{:zeno/arg {:name "Chris"
                                 :species "Canis familiaris"}
                      :zeno/op :zeno/insert-before
                      :zeno/path [:zeno/crdt :pets 0]}]
              :crdt (:crdt ret0)
              :crdt-schema pet-owner-schema
              :make-id make-id}
        arg2 {:cmds [{:zeno/arg {:name "Pat"
                                 :species "Canis familiaris"}
                      :zeno/op :zeno/insert-before
                      :zeno/path [:zeno/crdt :pets 0]}]
              :crdt (:crdt ret0)
              :crdt-schema pet-owner-schema
              :make-id make-id}
        ret1 (commands/process-cmds arg1)
        ret2 (commands/process-cmds arg2)
        new-ops (set/union (:ops ret1) (:ops ret2))
        merged-crdt (crdt/apply-ops {:crdt (:crdt ret0)
                                     :ops new-ops
                                     :schema pet-owner-schema})
        expected-value [{:name "Chris"
                         :species "Canis familiaris"}
                        {:name "Pat"
                         :species "Canis familiaris"}
                        {:name "Pinky"
                         :species "Felis catus"}
                        {:name "Fishy"
                         :species "Carassius auratus"}]]
    (is (= expected-value (crdt/get-value {:crdt merged-crdt
                                           :path [:pets]
                                           :schema pet-owner-schema})))))

(deftest test-merge-3-way-array-conflict
  (let [*next-id-num (atom 0)
        make-id #(let [n (swap! *next-id-num inc)]
                   (str "I" n))
        schema (l/array-schema l/string-schema)
        arg0 {:cmds [{:zeno/arg ["A" "B"]
                      :zeno/op :zeno/set
                      :zeno/path [:zeno/crdt]}]
              :crdt-schema schema
              :make-id make-id}
        ret0 (commands/process-cmds arg0)
        arg1 {:cmds [{:zeno/arg "1"
                      :zeno/op :zeno/insert-before
                      :zeno/path [:zeno/crdt 0]}]
              :crdt (:crdt ret0)
              :crdt-schema schema
              :make-id make-id}
        arg2 {:cmds [{:zeno/arg "2"
                      :zeno/op :zeno/insert-before
                      :zeno/path [:zeno/crdt 0]}]
              :crdt (:crdt ret0)
              :crdt-schema schema
              :make-id make-id}
        arg3 {:cmds [{:zeno/arg "3"
                      :zeno/op :zeno/insert-before
                      :zeno/path [:zeno/crdt 0]}]
              :crdt (:crdt ret0)
              :crdt-schema schema
              :make-id make-id}
        ret1 (commands/process-cmds arg1)
        ret2 (commands/process-cmds arg2)
        ret3 (commands/process-cmds arg3)
        new-ops (set/union (:ops ret1) (:ops ret2) (:ops ret3))
        merged-crdt (crdt/apply-ops {:crdt (:crdt ret0)
                                     :ops new-ops
                                     :schema schema})
        ;; Order of the first three items is determined by their add-ids.
        ;; Any ordering would be fine, as long as it is deterministic.
        ;; The important part is that all three appear before the original
        ;; sequence (["A" "B"]).
        expected-value ["2" "3" "1" "A" "B"]]
    (is (= expected-value (crdt/get-value {:crdt merged-crdt
                                           :path []
                                           :schema schema})))))

(deftest test-merge-3-way-array-conflict-multiple-peer-cmds
  (let [*next-id-num (atom 0)
        make-id #(let [n (swap! *next-id-num inc)]
                   (str "I" n))
        schema (l/array-schema l/string-schema)
        arg0 {:cmds [{:zeno/arg ["A" "B"]
                      :zeno/op :zeno/set
                      :zeno/path [:zeno/crdt]}]
              :crdt-schema schema
              :make-id make-id}
        ret0 (commands/process-cmds arg0)
        arg1 {:cmds [{:zeno/arg "X1"
                      :zeno/op :zeno/insert-before
                      :zeno/path [:zeno/crdt 0]}
                     {:zeno/arg "X2"
                      :zeno/op :zeno/insert-after
                      :zeno/path [:zeno/crdt 0]}]
              :crdt (:crdt ret0)
              :crdt-schema schema
              :make-id make-id}
        arg2 {:cmds [{:zeno/arg "Y1"
                      :zeno/op :zeno/insert-before
                      :zeno/path [:zeno/crdt 0]}
                     {:zeno/arg "Y2"
                      :zeno/op :zeno/insert-after
                      :zeno/path [:zeno/crdt 0]}]
              :crdt (:crdt ret0)
              :crdt-schema schema
              :make-id make-id}
        arg3 {:cmds [{:zeno/arg "Z1"
                      :zeno/op :zeno/insert-before
                      :zeno/path [:zeno/crdt 0]}
                     {:zeno/arg "Z2"
                      :zeno/op :zeno/insert-after
                      :zeno/path [:zeno/crdt 0]}]
              :crdt (:crdt ret0)
              :crdt-schema schema
              :make-id make-id}
        ret1 (commands/process-cmds arg1)
        ret2 (commands/process-cmds arg2)
        ret3 (commands/process-cmds arg3)
        new-ops (set/union (:ops ret1) (:ops ret2) (:ops ret3))
        merged-crdt (crdt/apply-ops {:crdt (:crdt ret0)
                                     :ops new-ops
                                     :schema schema})
        ;; Order of the first three pairs of items is determined by their
        ;; add-ids. Any ordering would be fine, as long as it is deterministic.
        ;; The important part is that all three appear before the original
        ;; sequence (["A" "B"]) and that the Xs, Ys, and Zs are not interleaved.
        expected-value ["Y1" "Y2" "Z1" "Z2" "X1" "X2" "A" "B"]]
    (is (= expected-value (crdt/get-value {:crdt merged-crdt
                                           :path []
                                           :schema schema})))))

(deftest test-merge-multiple-conflicts
  (let [*next-id-num (atom 0)
        make-id #(let [n (swap! *next-id-num inc)]
                   (str "I" n))
        schema (l/array-schema l/string-schema)
        arg0 {:cmds [{:zeno/arg ["A" "B" "C"]
                      :zeno/op :zeno/set
                      :zeno/path [:zeno/crdt]}]
              :crdt-schema schema
              :make-id make-id}
        ret0 (commands/process-cmds arg0)
        arg1 {:cmds [{:zeno/arg "XF"
                      :zeno/op :zeno/insert-before
                      :zeno/path [:zeno/crdt 0]}
                     {:zeno/arg "XL"
                      :zeno/op :zeno/insert-after
                      :zeno/path [:zeno/crdt -1]}]
              :crdt (:crdt ret0)
              :crdt-schema schema
              :make-id make-id}
        arg2 {:cmds [{:zeno/arg "YF"
                      :zeno/op :zeno/insert-before
                      :zeno/path [:zeno/crdt 0]}
                     {:zeno/arg "YL"
                      :zeno/op :zeno/insert-after
                      :zeno/path [:zeno/crdt -1]}]
              :crdt (:crdt ret0)
              :crdt-schema schema
              :make-id make-id}
        arg3 {:cmds [{:zeno/arg "ZF"
                      :zeno/op :zeno/insert-before
                      :zeno/path [:zeno/crdt 0]}
                     {:zeno/arg "ZL"
                      :zeno/op :zeno/insert-after
                      :zeno/path [:zeno/crdt -1]}]
              :crdt (:crdt ret0)
              :crdt-schema schema
              :make-id make-id}
        ret1 (commands/process-cmds arg1)
        ret2 (commands/process-cmds arg2)
        ret3 (commands/process-cmds arg3)
        new-ops (set/union (:ops ret1) (:ops ret2) (:ops ret3))
        merged-crdt (crdt/apply-ops {:crdt (:crdt ret0)
                                     :ops new-ops
                                     :schema schema})
        expected-value ["XF" "YF" "ZF" "A" "B" "C" "XL" "YL" "ZL"]]
    (is (= expected-value (crdt/get-value {:crdt merged-crdt
                                           :make-id make-id
                                           :path []
                                           :schema schema})))))

(deftest test-nested-merge-conflict
  (let [arg0 {:cmds [{:zeno/arg {:name "Bill"
                                 :pets [{:name "Pinky"
                                         :species "Felis catus"}
                                        {:name "Fishy"
                                         :species "Carassius auratus"}]}
                      :zeno/op :zeno/set
                      :zeno/path [:zeno/crdt]}]
              :crdt-schema pet-owner-schema}
        ret0 (commands/process-cmds arg0)
        arg1 {:cmds [{:zeno/arg "Goldy"
                      :zeno/op :zeno/set
                      :zeno/path [:zeno/crdt :pets -1 :name]}]
              :crdt (:crdt ret0)
              :crdt-schema pet-owner-schema
              :sys-time-ms (u/str->long "1640205282000")}
        arg2 {:cmds [{:zeno/arg "Herman"
                      :zeno/op :zeno/set
                      :zeno/path [:zeno/crdt :pets -1 :name]}]
              :crdt (:crdt ret0)
              :crdt-schema pet-owner-schema
              :sys-time-ms (u/str->long "1640205282999")}
        ret1 (commands/process-cmds arg1)
        ret2 (commands/process-cmds arg2)
        merged-crdt (crdt/apply-ops {:crdt (:crdt ret0)
                                     :ops (set/union (:ops ret1) (:ops ret2))
                                     :schema pet-owner-schema})]
    ;; We expect "Herman" because its sys-time-ms is later
    (is (= "Herman" (crdt/get-value {:crdt merged-crdt
                                     :path [:pets -1 :name]
                                     :schema pet-owner-schema})))))

(deftest test-set-into-empty-array
  (let [arg {:cmds [{:zeno/arg "1"
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt]}]
             :crdt-schema (l/array-schema l/string-schema)}]
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"arg is not sequential"
         (commands/process-cmds arg)))))
