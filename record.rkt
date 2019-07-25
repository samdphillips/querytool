#lang racket/base

(require racket/match)

(provide (struct-out record)
         
         record-field-extend
         record-field-remove
         record-field-rename
         record-field-value
         
         records-join)

(define schema? hash?)

(struct record
  [schema values]
  #:transparent)

(define (record-field-value rec field-name)
  (let ([i (hash-ref (record-schema rec) field-name)])
    (vector-ref (record-values rec) i)))

(define (record-field-extend rec field-name field-value)
  (match-define (record s v) rec)
  (let ([j (vector-length v)])
    (record (hash-set (record-schema rec)
                      field-name
                      j)
            (build-vector (add1 j)
                          (lambda (i)
                            (cond
                              [(= i j) field-value]
                              [else
                               (vector-ref v i)]))))))

(define (record-field-remove rec field-name)
  (match-define (record s v) rec)
  (let ([j (hash-ref s field-name)])
    (record (for/fold ([s (hasheq)]) ([(k v) (in-hash s)])
              (cond
                [(eq? k field-name) s]
                [(< v j) (hash-set s k v)]
                [else
                 (hash-set s k (sub1 v))]))
            (build-vector (sub1 (vector-length v))
                          (lambda (i)
                            (let ([k (if (< i j) i (add1 i))])
                              (vector-ref v k)))))))

(define (record-field-rename rec old new)
  (match-define (record s v) rec)
  (define i (hash-ref s old))
  (record (hash-set (hash-remove s old) new i) v))

(define (records-join f r0 r1)
  (match-define (record s0 v0) r0)
  (match-define (record s1 v1) r1)
  (define len0 (vector-length v0))
  (define j (hash-ref s1 f))
  (define vals
    (build-vector
     (+ len0 (vector-length v1) -1)
     (lambda (i)
       (and (< i len0) (vector-ref v0 i)))))
  (define s
    (for/fold ([s s0]) ([(k v) (in-hash s1)])
      (cond
        [(eq? k f) s]
        [(< v j)
         (let ([i (+ v len0)])
           (vector-set! vals i (vector-ref v1 v))
           (hash-set s k i))]
        [else
         (let ([i (+ v len0 -1)])
           (vector-set! vals i (vector-ref v1 v))
           (hash-set s k (+ v len0 -1)))])))
  (record s vals))