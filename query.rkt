#lang racket/base

(require racket/generic
         racket/match
         racket/stream

         threading

         "record.rkt")

(require racket/format)

(define-generics relation
  [relation-next relation]
  [relation-close relation]

  #:fallbacks
  [(define relation-close void)])

(define-generics qop
  [qop-exec qop]
  #:derive-property prop:sequence
  (lambda (q)
    (let ([rel (qop-exec q)])
      (define (produce)
        (let ([v (relation-next rel)])
          (cond
            [(not v) (relation-close rel)
                     #f]
            [else v])))
      (in-producer produce #f))))

(define (query-execute q)
  (define rel (qop-exec q))
  (let exec ([rec (relation-next rel)])
    (cond
      [(not rec) (relation-close rel)]
      [else
       (exec (relation-next rel))])))

#|
########################################################################
########################################################################

         ####  ######  ####  #    # ###### #    #  ####  ######
        #      #      #    # #    # #      ##   # #    # #
         ####  #####  #    # #    # #####  # #  # #      #####
             # #      #  # # #    # #      #  # # #      #
        #    # #      #   #  #    # #      #   ## #    # #
         ####  ######  ### #  ####  ###### #    #  ####  ######

########################################################################
########################################################################
|#

(struct op:sequence
  [seq]
  #:methods gen:qop
  [(define (qop-exec q)
     (define-values (more? next)
       (sequence-generate (op:sequence-seq q)))
     (rel:sequence more? next))])

(struct rel:sequence
  [more? next]
  #:methods gen:relation
  [(define (relation-next rel)
     (match-define (rel:sequence more? next) rel)
     (and (more?) (next)))])


#|
########################################################################
########################################################################

       ###### # #      ######       #####  ######   ##   #####
       #      # #      #            #    # #       #  #  #    #
       #####  # #      #####  ##### #    # #####  #    # #    #
       #      # #      #            #####  #      ###### #    #
       #      # #      #            #   #  #      #    # #    #
       #      # ###### ######       #    # ###### #    # #####

########################################################################
########################################################################
|#

(struct op:file-read
  [fname
   reader]
  #:methods gen:qop
  [(define (qop-exec q)
     (match-define
       (op:file-read fname read) q)
     (define port (open-input-file fname))
     (rel:file-read port read))])

(struct rel:file-read
  [port read]
  #:methods gen:relation
  [(define (relation-next rel)
     (match-define
       (rel:file-read port read) rel)
     (read port))

   (define (relation-close rel)
     (close-input-port (rel:file-read-port rel)))])

(define (line-reader in-port)
  (define line (read-line in-port))
  (cond
    [(eof-object? line) #f]
    [else
     (record (hasheq 'raw 0) (vector line))]))

#|
########################################################################
########################################################################

                        #      # #    # # #####
                        #      # ##  ## #   #
                        #      # # ## # #   #
                        #      # #    # #   #
                        #      # #    # #   #
                        ###### # #    # #   #

########################################################################
########################################################################
|#

(struct op:limit
  [qop amt]
  #:methods gen:qop
  [(define/generic qop-exec^ qop-exec)

   (define (qop-exec q)
     (define amt (op:limit-amt q))
     (rel:limit (qop-exec^ (op:limit-qop q))
                (lambda (i) (< i amt))
                0))])

(struct rel:limit
  [rel more? (posn #:mutable)]
  #:methods gen:relation
  [(define/generic relation-next^ relation-next)
   (define/generic relation-close^ relation-close)

   (define (relation-next rel)
     (match-define
       (rel:limit rel^ more? posn) rel)
     (and (more? posn)
          (begin
            (set-rel:limit-posn! rel (add1 posn))
            (relation-next^ rel^))))

   (define (relation-close rel)
     (relation-close^ (rel:limit-rel rel)))])

#|
########################################################################
########################################################################

                         #    #   ##   #####
                         ##  ##  #  #  #    #
                         # ## # #    # #    #
                         #    # ###### #####
                         #    # #    # #
                         #    # #    # #

########################################################################
########################################################################
|#

(struct op:map
  [qop func]
  #:methods gen:qop
  [(define/generic qop-exec^ qop-exec)
   (define (qop-exec q)
     (match-define (op:map q^ func) q)
     (rel:map (qop-exec^ q^) func))])

(struct rel:map
  [rel func]
  #:methods gen:relation
  [(define/generic relation-next^  relation-next)
   (define/generic relation-close^ relation-close)

   (define (relation-next rel)
     (match-define (rel:map rel^ func) rel)
     (let ([v (relation-next^ rel^)])
       (and v (func v))))

   (define (relation-close rel)
     (relation-close^ (rel:map-rel rel)))])

(define (op:extract-field qop src dest convert)
  (define (extract rec)
    (let ([v (record-field-value rec src)])
      (record-field-extend rec dest (convert v))))
  (op:map qop extract))

(define (op:remove-field qop field-name)
  (op:map qop (lambda~> (record-field-remove field-name))))

(define (op:rename-field qop old new)
  (op:map qop (lambda~> (record-field-rename old new))))

#|
########################################################################
########################################################################

                  ###### # #      ##### ###### #####
                  #      # #        #   #      #    #
                  #####  # #        #   #####  #    #
                  #      # #        #   #      #####
                  #      # #        #   #      #   #
                  #      # ######   #   ###### #    #

########################################################################
########################################################################
|#

(struct op:filter
  [qop pred?]
  #:methods gen:qop
  [(define/generic qop-exec^ qop-exec)
   (define (qop-exec q)
     (match-define (op:filter q^ pred?) q)
     (rel:filter (qop-exec^ q^) pred?))])

(struct rel:filter
  [rel pred?]
  #:methods gen:relation
  [(define/generic relation-next^ relation-next)
   (define/generic relation-close^ relation-close)

   (define (relation-next rel)
     (match-define (rel:filter rel^ pred?) rel)
     (let ([v (relation-next^ rel^)])
       (and v (if (pred? v) v (relation-next rel)))))

   (define (relation-close rel)
     (relation-close^ (rel:filter-rel rel)))])
#|
########################################################################
########################################################################

       #    #   ##    ####  #    #            #  ####  # #    #
       #    #  #  #  #      #    #            # #    # # ##   #
       ###### #    #  ####  ###### #####      # #    # # # #  #
       #    # ######      # #    #            # #    # # #  # #
       #    # #    # #    # #    #       #    # #    # # #   ##
       #    # #    #  ####  #    #        ####   ####  # #    #

########################################################################
########################################################################
|#

(struct op:hash-join
  [join-field make-default-record lqop rqop]
  #:methods gen:qop
  [(define/generic qop-exec^ qop-exec)

   (define (qop-exec q)
     (match-define
       (op:hash-join join-field make-default lq rq) q)
     (define ht
       (for/fold ([ht (hash)]) ([rec lq])
         (hash-set ht (record-field-value rec join-field) rec)))
     (rel:map (qop-exec^ rq)
              (lambda (rrec)
                (define join (record-field-value rrec join-field))
                (define lrec
                  (hash-ref ht join (lambda () (make-default join))))
                (records-join join-field rrec lrec))))])

#|
########################################################################
########################################################################

              #####  #####  # #    # ##### ###### #####
              #    # #    # # ##   #   #   #      #    #
              #    # #    # # # #  #   #   #####  #    #
              #####  #####  # #  # #   #   #      #####
              #      #   #  # #   ##   #   #      #   #
              #      #    # # #    #   #   ###### #    #

########################################################################
########################################################################
|#

(define (op:record-printer qop)
  (op:map qop println))

(struct op:table-printer
  [qop fields format]
  #:methods gen:qop
  [(define/generic qop-exec^ qop-exec)
   (define (qop-exec q)
     (match-define (op:table-printer q^ fields format) q)
     (displayln (format fields))
     (rel:table-printer (qop-exec^ q^)
                        (lambda (rec)
                          (apply format
                                 fields
                                 (for/list ([f (in-list fields)])
                                   (record-field-value rec f))))))])

(struct rel:table-printer
  [rel format]
  #:methods gen:relation
  [(define/generic relation-next^ relation-next)
   (define/generic relation-close^ relation-close)

   (define (relation-next rel)
     (match-define (rel:table-printer rel^ format) rel)
     (let ([v (relation-next^ rel^)])
       (and v (displayln (format v)) v)))])

#|
########################################################################
########################################################################

                      ####  #####  #      # #####
                     #      #    # #      #   #
                      ####  #    # #      #   #
                          # #####  #      #   #
                     #    # #      #      #   #
                      ####  #      ###### #   #

########################################################################
########################################################################
|#

(define (op:split q . f*)
  (let ([s (sequence->stream q)])
    (for/list ([f (in-list f*)])
      (f (op:sequence s)))))

#|
########################################################################
########################################################################

       ##    ####   ####  #####  ######  ####    ##   ##### ######
      #  #  #    # #    # #    # #      #    #  #  #    #   #
     #    # #      #      #    # #####  #      #    #   #   #####
     ###### #  ### #  ### #####  #      #  ### ######   #   #
     #    # #    # #    # #   #  #      #    # #    #   #   #
     #    #  ####   ####  #    # ######  ####  #    #   #   ######

########################################################################
########################################################################
|#

(struct op:aggregate
  [qop field-name val-func init-val agg-func]
  #:methods gen:qop
  [(define (qop-exec q)
     (match-define
       (op:aggregate q^ field-name val-func init-val agg-func) q)
     (define agg-val
       (for/fold ([v init-val]) ([rec q^])
         (agg-func v (val-func rec))))
     (let ([more? #t])
       (rel:sequence (lambda () more?)
                     (lambda ()
                       (set! more? #f)
                       (record (hasheq field-name 0)
                               (vector agg-val))))))])

#|
########################################################################
########################################################################

                 ##   #####  #####  ###### #    # #####
                #  #  #    # #    # #      ##   # #    #
               #    # #    # #    # #####  # #  # #    #
               ###### #####  #####  #      #  # # #    #
               #    # #      #      #      #   ## #    #
               #    # #      #      ###### #    # #####

########################################################################
########################################################################
|#

(struct op:append
  [qop*]
  #:methods gen:qop
  [(define/generic qop-exec^ qop-exec)
   (define (qop-exec q)
     (define q^* (op:append-qop* q))
     (match q^*
       [(list q^)         (qop-exec^ q^)]
       [(list q^ q^* ...) (rel:append (qop-exec^ q^) q^*)]))])

(struct rel:append
  [rel qop*]
  #:mutable
  #:methods gen:relation
  [(define/generic relation-next^ relation-next)
   (define/generic relation-close^ relation-close)

   (define (relation-next rel)
     (match-define (rel:append rel^ q) rel)
     (let ([v (relation-next^ rel^)])
       (match* (v q)
         [(#f (list)) #f]
         [(#f (list q q* ...))
          (relation-close^ rel^)
          (set-rel:append-rel! rel (qop-exec q))
          (set-rel:append-qop*! rel q*)
          (relation-next rel)]
         [(v _) v])))

   (define (relation-close rel)
     (relation-close^ (rel:append-rel rel)))])



(provide line-reader

         op:aggregate
         op:append
         op:extract-field
         op:file-read
         op:filter
         op:hash-join
         op:limit
         op:record-printer
         op:remove-field
         op:rename-field
         op:sequence
         op:split
         op:table-printer

         query-execute)
