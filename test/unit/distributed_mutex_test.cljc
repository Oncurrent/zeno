(ns unit.distributed-mutex-test
  (:require
   [clojure.core.async :as ca]
   [clojure.test :as t :refer [deftest is]]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster :as l]
   [oncurrent.zeno.distributed-mutex :as mut]
   [oncurrent.zeno.schemas :as schemas]
   [oncurrent.zeno.storage :as storage]
   [oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log])
  #?(:clj
     (:import
      (clojure.lang ExceptionInfo))))

(deftest test-bad-names
  (let [storage (storage/make-storage)]
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"invalid. It must be 3-200 characters long"
         (mut/make-distributed-mutex-client "m" storage)))
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"invalid"
         (mut/make-distributed-mutex-client (apply str (take 201 (repeat "x")))
                                            storage)))
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"invalid"
         (mut/make-distributed-mutex-client "name with spaces" storage)))))

(deftest test-acquisition-and-release
  (au/test-async
   3000
   (ca/go
     (let [storage (storage/make-storage)
           on-acq-ch (ca/chan)
           on-rel-ch (ca/chan)
           opts {:lease-length 500
                 :on-acquire (fn [_]
                               (ca/put! on-acq-ch "Acquired"))
                 :on-release (fn [_]
                               (ca/put! on-rel-ch "Released"))
                 :refresh-ratio 2}
           client (mut/make-distributed-mutex-client "my-mutex" storage opts)]
       (ca/<! (ca/timeout 100))
       (mut/stop! client)
       (is (= "Acquired" (ca/<! on-acq-ch)))
       (is (= "Released" (ca/<! on-rel-ch)))))))

(deftest test-contention
  (au/test-async
   3000
   (ca/go
     (let [storage (storage/make-storage)
           on-acq-ch1 (ca/chan)
           on-rel-ch1 (ca/chan)
           on-acq-ch2 (ca/chan)
           on-rel-ch2 (ca/chan)
           opts1 {:lease-length-ms 500
                  :on-acquire (fn [_]
                                (ca/put! on-acq-ch1 "C1 Acquired"))
                  :on-release (fn [_]
                                (ca/put! on-rel-ch1 "C1 Released"))
                  :refresh-ratio 2}
           opts2 (assoc opts1
                        :on-acquire (fn [_]
                                      (ca/put! on-acq-ch2 "C2 Acquired"))
                        :on-release (fn [_]
                                      (ca/put! on-rel-ch2 "C2 Released")))
           mutex-name "the-mutex"
           client1 (mut/make-distributed-mutex-client mutex-name storage opts1)
           _ (ca/<! (ca/timeout 100))
           client2 (mut/make-distributed-mutex-client mutex-name storage opts2)]
       (ca/<! (ca/timeout 200))
       (is (= "C1 Acquired" (ca/<! on-acq-ch1)))
       (mut/stop! client1)
       (is (= "C1 Released" (ca/<! on-rel-ch1)))
       (is (= "C2 Acquired" (ca/<! on-acq-ch2)))
       (mut/stop! client2)
       (is (= "C2 Released" (ca/<! on-rel-ch2)))))))

(deftest test-watch-only
  (au/test-async
   3000
   (ca/go
     (let [storage (storage/make-storage)
           on-acq-ch (ca/chan)
           on-rel-ch (ca/chan)
           on-stale-ch (ca/chan)
           opts1 {:lease-length-ms 500
                  :on-acquire (fn [_]
                                (ca/put! on-acq-ch "C1 acquired"))
                  :on-release (fn [_]
                                (ca/put! on-rel-ch "C1 released"))
                  :refresh-ratio 2}
           opts2 (assoc opts1
                        :on-stale (fn [_]
                                    (ca/put! on-stale-ch "Mutex went stale"))
                        :watch-only? true)
           mutex-name "the-mutex"
           client1 (mut/make-distributed-mutex-client mutex-name storage opts1)
           _ (ca/<! (ca/timeout 100))
           client2 (mut/make-distributed-mutex-client mutex-name storage opts2)]
       (ca/<! (ca/timeout 200))
       (is (= "C1 acquired" (ca/<! on-acq-ch)))
       (mut/stop! client1)
       (is (= "C1 released" (ca/<! on-rel-ch)))
       (is (= "Mutex went stale" (ca/<! on-stale-ch)))
       (mut/stop! client2)))))
