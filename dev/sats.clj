(ns sats
  (:require [clojure.core.async
             :refer [close! tap >!! mult go-loop timeout alts!]
             :as a]))


(defn ctrl
  []
  (let [c (chan)]
    {:control-chan c :ground (mult c)}))



(defn shutdown
  [{:keys [control-chan] :as ctrl}]
  (close! control-chan))



(defn listen
  [{:keys [ground] :as ctrl} c]
  (tap ground c))



(defn printer
  "Prints message sent in the input chan. If message sent
  is :shutdown, it shutsdown all printers. Listens for control signal
  in control-chan and shuts down"
  ([n ctrl] (let [in (chan)] (printer in n ctrl) in))
  ([in n ctrl]
   (let [shutdown-ch (listen ctrl (chan))]
     (go-loop []
       (let [[v p] (alts! [shutdown-ch in])]
         (cond
           (and (= p in) (= v :shutdown))
           (do (prn n ":" v) (shutdown ctrl))

           (and (= p in))
           (do (prn n ":" v) (recur))

           :else
           (do
             (prn n ":Got shutdown signal")
             (close! in))))))))


(comment

  (def cc (ctrl))


  (def c1 (printer "one" cc))

  (def c2 (printer "two" cc))


  (>!! c1 :helllo)

  (>!! c2 :helllo)


  (>!! c2 :shutdown)



  )
