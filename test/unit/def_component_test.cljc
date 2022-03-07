(ns unit.def-component-test
  (:require
   [clojure.test :as t :refer [deftest is]]
   [com.oncurrent.zeno.client.macro-impl :as macro-impl]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log])
  #?(:clj
     (:import
      (clojure.lang ExceptionInfo))))

(deftest test-parse-def-component-args-no-docstring
  (let [arglist '[zc a b]
        sub-map '{x [:local :c]}
        args [arglist sub-map]
        expected {:sub-map sub-map
                  :arglist arglist
                  :body nil}]
    (is (= expected (macro-impl/parse-def-component-args 'foo args)))))

(deftest test-parse-def-component-args-with-docstring
  (let [docstring "My component"
        arglist '[zc a b]
        sub-map '{x [:local :c]}
        args [docstring arglist sub-map]
        expected {:docstring docstring
                  :sub-map sub-map
                  :arglist arglist
                  :body nil}]
    (is (= expected (macro-impl/parse-def-component-args 'foo args)))))

(deftest test-parse-def-component-args-bad-arg
  (is (thrown-with-msg?
       #?(:clj ExceptionInfo :cljs js/Error)
       #"The argument list must be a vector"
       (macro-impl/parse-def-component-args 'foo nil))))

(deftest test-check-arglist-no-zc
  (is (thrown-with-msg?
       #?(:clj ExceptionInfo :cljs js/Error)
       #"First argument must be `zc`"
       (macro-impl/check-arglist 'foo true '[]))))

(deftest test-check-arglist-no-zc-2
  (is (thrown-with-msg?
       #?(:clj ExceptionInfo :cljs js/Error)
       #"First argument must be `zc`"
       (macro-impl/check-arglist 'foo true '[x]))))

(deftest test-check-arglist
  (is (thrown-with-msg?
       #?(:clj ExceptionInfo :cljs js/Error)
       #"The argument list must be a vector"
       (macro-impl/check-arglist 'foo false '()))))

(deftest test-repeat-symbol
  (is (thrown-with-msg?
       #?(:clj ExceptionInfo :cljs js/Error)
       #"Illegal repeated symbol"
       (macro-impl/parse-def-component-args
        'foo ['[zc a] '{a [:local :a]}]))))
