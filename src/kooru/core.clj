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
  "Abstract many-to-many component.
  `id`      - an identifier for this component (used in logs etc).
  `inputs`  - map containining input channels keyed by name
  `f`       - function of arity 3 [state input msg].
              `state`  - persistent state of this component. Set to nil for the
                         first time.
              `input`  - name of the channel from which the message was read.
              `msg`    - message read from the input channels.
              f must return a 3-tuple [state' output msg']
              `state'` - updated state.
              `output` - name of the output channel to which msg' prime must
                         be sent to.
              `msg'    - output message
  `outputs` - map containing output channels keyed by name.
  `ctrl`    - ctrl provides mechanism necessary to abort incase of an
              exception and also listen to abort signal from other components
              in the pipeline this component is a part of.

  shutdown behaviour - if all the input channels are closed, the component
  gracefully shuts down by closing all the output channels, triggering a
  cascading shutdown of downstream components of the pipeline."
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
              (cond
                ;; on abort, just exit
                (= p abort-chan)
                (log/info "Abort received. Shutting down :" id)

                ;; remove any inputs that were closed.
                (nil? v)
                (let [i (disj input-chans p)]
                  (if-not (= i #{abort-chan})
                    (recur i state)
                    (do
                      (->> outputs vals (map close!) doall)
                      (log/info "No more channels to listen to. Shutting down."))))

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
          (abort ctrl))))


    {:id id :inputs inputs :outputs outputs}))




;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;                     ---==| C O M P O N E N T S |==----                     ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn many-to-one
  "many-to-one component - reads messages from many channels, calls function f
  with the message, and routes the output messate to a single output channel.

  `id`        - an identifier for this component (used in logs etc).
  `inputs`    - map containining input channels keyed by name
  `f`         -   function of arity 3 [state input msg].
                `state`  - persistent state of this component. Set to nil for the
                         first time.
                `input`  - name of the channel from which the message was read.
                `msg`    - message read from the input channels.
                f must return a tuple [state' msg']
                `state'` - updated state.
                `msg'    - output message
  `output-id` - an optional name for the output channel. defaulted to :default
  `output`    - output channel.
  `ctrl`      - ctrl provides mechanism necessary to abort incase of an
                exception and also listen to abort signal from other components
                in the pipeline this component is a part of."
  ([id inputs f output ctrl]
   (many-to-one id inputs f :default output ctrl))
  ([id inputs f output-id output ctrl]
   (component*
    id
    inputs
    (fn [state input msg]
      (let [[state' msg'] (f state input msg)]
        [state' output-id msg']))
    {output-id output}
    ctrl)))



(defn many-to-many-broadcast
  "many-to-many component - reads messages from many input channels,
  calls function f with the message, and broadcasts the output message
  to all the output channels supplied.

  `id`      - an identifier for this component (used in logs etc).
  `inputs`  - map containining input channels keyed by name
  `f`       - function of arity 3 [state input msg].
              `state`  - persistent state of this component. Set to nil for the
                         first time.
              `input`  - name of the channel from which the message was read.
              `msg`    - message read from the input channels.
              f must return a tuple [state' msg']
              `state'` - updated state.
              `msg'    - output message
  `outputs` - map of output channels keyed by name.
  `ctrl`    - ctrl provides mechanism necessary to abort incase of an
              exception and also listen to abort signal from other components
              in the pipeline this component is a part of."
  [id inputs f outputs ctrl]
  ;; the broadcast is setup using an internal output channel which
  ;; will be passed to the abstract component.
  (let [internal-output (chan)
        broadcast       (mult internal-output)]
    (doall (map (partial tap broadcast) outputs))
    (component*
     id
     inputs
     (fn [state input msg]
       (let [[state' msg'] (f state input msg)]
         [state' :broadcast msg']))
     {:broadcast internal-output}
     ctrl)))



(def many-to-many component*)



(defn one-to-one
  "one-to-one component - reads messages from a single  input channel, calls
  function f with the message, and routes the output message to a single output
  channel.

  `id`        - an identifier for this component (used in logs etc).
  `input-id`  - optional name for the input channel. defaulted to :default
  `input`     - input channel
  `f`         - function of arity 2 [state msg].
                `state`  - persistent state of this component. Set to nil for the
                           first time.
                `msg`    - message read from the input channels.
                f must return a tuple [state' msg']
                `state'` - updated state.
                `msg'    - output message
  `output-id`- optional name for the output channel. defaulted to :default
  `output`   - output channel.
  `ctrl`     - ctrl provides mechanism necessary to abort incase of an
               exception and also listen to abort signal from other components
               in the pipeline this component is a part of."
  ([id input f output ctrl]
   (one-to-one id :default input f :default output ctrl))
  ([id input-id input f output-id output ctrl]
   (component*
    id
    {input-id input}
    (fn [state input msg]
      (let [[state' msg'] (f state input-id msg)]
        [state' output-id msg']))
    {output-id output}
    ctrl)))



(defn one-to-many
  "one-to-many component - reads messages from a single input channel,
  calls function f with the message, and routes the output message to
  one of the output channels based on the response returned by f.

  `id`        - an identifier for this component (used in logs etc).
  `input-id`  - optional name for the input channel. defaulted to :default
  `input`     - input channel
  `f`         - function of arity 2 [state msg].
                `state`  - persistent state of this component. Set to nil for the
                           first time.
                `msg`    - message read from the input channels.
                f must return a 3-tuple [state' output msg']
                `state'` - updated state.
                `output` - name of the output channel to send msg' to.
                `msg'    - output message
  `outputs`   - map of output channels keyed by name.
  `ctrl`      - ctrl provides mechanism necessary to abort incase of an
                exception and also listen to abort signal from other components
                in the pipeline this component is a part of."
  ([id input f outputs ctrl]
   (one-to-one id :default input f outputs ctrl))
  ([id input-id input f outputs ctrl]
   (component*
    id
    {input-id input}
    (fn [state input msg]
      (let [[state' output msg'] (f state msg)]
        [state' output msg']))
    outputs
    ctrl)))



(defn one-to-many-broadcast
  "one-to-many component - reads messages from a single input channel,
  calls function f with the message, and broadcasts the output message
  to all the output channels supplied.

  `id`      - an identifier for this component (used in logs etc).
  `input`   - input channel
  `f`       - function of arity 2 [state msg].
              `state`  - persistent state of this component. Set to nil for the
                         first time.
              `msg`    - message read from the input channels.
              f must return a tuple [state' msg']
              `state'` - updated state.
              `msg'    - output message
  `outputs` - map of output channels keyed by name.
  `ctrl`    - ctrl provides mechanism necessary to abort incase of an
              exception and also listen to abort signal from other components
              in the pipeline this component is a part of."
  [id input f outputs ctrl]
  ;; the broadcast is setup using an internal output channel which
  ;; will be passed to the abstract component.
  (let [internal-output (chan)
        broadcast       (mult internal-output)]
    (doall (map (partial tap broadcast) outputs))
    (component*
     id
     {:default input}
     (fn [state input msg]
       (let [[state' msg'] (f state msg)]
         [state' :broadcast msg']))
     {:broadcast internal-output}
     ctrl)))




(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (println "Hello, World!"))


(comment


  (defn printer
    [c m]
    (go-loop []
      (if-let [msg (<! c)]
        (do (prn m msg) (recur))
        (prn "PRINTER: " m " SHUTDOWN!!"))))



  (defn sorting-hat
    ([s m]
     [s (-> (Math/abs (.hashCode m))
            (mod 4)
            ((partial nth
                [:gryffindor :hufflepuff :ravenclaw :slytherin])))])

    ([s i m]
     (let [house  (-> (Math/abs (.hashCode m))
                      (mod 4)
                      ((partial nth
                          [:gryffindor :hufflepuff :ravenclaw :slytherin])))]
       [s house m])))


;;;;;;;;; abstract component
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

;;;;;;;;;; many-to-one
  (def _ctrl (ctrl))

  (def inputs {:one (chan) :two (chan)})

  (def output (chan))

  (def m
    (many-to-one
     "sorting-hat"
     inputs
     (fn [state input msg]
       [state
        (-> (Math/abs (.hashCode m))
            (mod 4)
            ((partial nth [:gryffindor :hufflepuff :ravenclaw :slytherin])))])
     :house
     output
     _ctrl))

  m

  (printer output "SORTING HAT:")

  (>!! (:one inputs) "Peter Parker")

  (>!! (:two inputs) "Harry Potter")

  (close! (:two inputs))

  (close! (:one inputs))

  (abort _ctrl)

 ;;;;;;;;;; one-to-one
  (def _ctrl (ctrl))

  (def input (chan))

  (def output (chan))

  (def o
    (one-to-one
     "sorting-hat"
     :one input
     (fn [state msg]
       (let [{:keys [count]} state
             cnt (inc count)]
         [(assoc state :count cnt)
          (-> (Math/abs (.hashCode m))
              (mod 4)
              ((partial nth [:gryffindor :hufflepuff :ravenclaw :slytherin]))
              (str " , called " cnt " times"))]))
     :house output
     _ctrl))

  m

  (printer output "SORTING HAT:")

  (>!! (:one inputs) "Peter Parker")

  (>!! (:two inputs) "Harry Potter")

  (close! (:two inputs))

  (close! (:one inputs))

  (abort _ctrl)


  ;;;;;;;broadcast
  (def _ctrl (ctrl))

  (def inputs {:one (chan) :two (chan)})

  (def outputs [(chan) (chan) (chan) (chan)])

  (def b
    (many-to-many-broadcast
     "sorting-hat"
     inputs
     (fn [state input msg]
       (prn "Got " msg)
       (let [{:keys [count]} state
             cnt (if count (inc count) 1)]
         [(assoc state :count cnt)
          (-> (Math/abs (.hashCode m))
              (mod 4)
              ((partial nth [:gryffindor :hufflepuff :ravenclaw :slytherin]))
              (str " , called " cnt " times"))]))
     outputs
     _ctrl))


  m

  (map #(printer % "SH") outputs)

  (>!! (:one inputs) "Peter Parker")

  (>!! (:two inputs) "Harry Potter")

  (close! (:two inputs))

  (close! (:one inputs))

  (abort _ctrl)







  ;;END REPL
  )
