(ns kooru.core
  (:require [clojure.core.async
             :refer [close! tap >!! <! mult go-loop thread timeout alts!! chan]
             :as a]
            [clojure.set :refer [map-invert]]
            [clojure.tools.logging :as log])
  (:gen-class))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;                        ---==| C O N T R O L |==----                        ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn ctrl
  []
  (let [c (chan)]
    {:control-chan c :ground (mult c)}))



(defn abort
  [{:keys [control-chan] :as ctrl}]
  (close! control-chan))



(defn listen
  [{:keys [ground] :as ctrl} c]
  (tap ground c))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;             ---==| A B S T R A C T   C O M P O N E N T |==----             ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn component*
  "Abstract many-to-one component.
  `inputs`  - map containining input channels keyed by name
  `f`       - function of arity 3 [state input msg].
              `state`  -
              `input`  - name of the channel from which the message was read.
              `msg`    - message read from the input channels.
              f must return a 3-tuple [state' output msg']
              `state'` - updated state.
              `output` - name of the output channel to which msg' prime must
                         be sent to.
              `msg'    -
  `outputs` - map containing output channels keyed by name.
  `ctrl`    - ctrl provides mechanism necessary to abort incase of an
              exception and also listen to abort signal from other components
              in the pipeline this component is a part of."
  [id inputs f outputs ctrl]
  (let [abort-chan  (listen ctrl (chan))]
    (a/thread
      (try
        (let [input-chans (->> inputs vals (into #{abort-chan}))
              inputs'     (map-invert inputs)
              outputs'    (map-invert outputs)]
          (loop [input-chans input-chans
                 state       nil]
            (let [[v p] (alts!! (vec input-chans))]
              (log/info "Got " v p)
              (cond
                ;; on abort, just exit
                (= p abort-chan)
                (log/info "Abort received. Shutting down :" id)

                ;; remove any inputs that were closed.
                (nil? v)
                (let [i (disj input-chans p)]
                  (if-not (= i #{abort-chan})
                    (recur i state)
                    (log/info "No more channels to listen to. Shutting down.")))

                ;; call f with the message received.
                :else
                (let [[state' output msg']
                      (f state (get inputs' p) v)
                      out-chan (get outputs output)]
                  (when (and output msg')
                    (>!! out-chan msg'))

                  (recur input-chans state'))))))

        (catch Throwable t
          (log/error t "Error in component :" id ". Aborting.")
          (abort ctrl))))))




(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (println "Hello, World!"))


(comment


  (defn printer
    [c m]
    (go-loop []
      (when-let [msg (<! c)]
        (prn m msg)
        (recur))))



  (defn sorting-hat
    [s i m]
    (let [house  (-> (Math/abs (.hashCode m))
                     (mod 4)
                     ((partial nth
                         [:gryffindor :hufflepuff :ravenclaw :slytherin])))]
      [s house m]))


  (def _ctrl (ctrl))

  (def inputs {:one (chan) :two (chan)})

  (def outputs
    {:gryffindor (chan) :hufflepuff (chan) :ravenclaw (chan) :slytherin (chan)})

  (doseq [[h c] outputs]
    (printer c (str h ">")))

  (component* "sorting-hat" inputs sorting-hat outputs _ctrl)


  (doall (map close! (vals outputs)))

  (>!! (:one inputs) "Peter Parker")

  (>!! (:two inputs) "Harry Potter")

  (close! (:two inputs))

  (close! (:one inputs))

  (abort _ctrl)


  ;;END REPL
  )
