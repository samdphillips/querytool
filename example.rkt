#lang racket/base

(require racket/format
         racket/match
         racket/string

         threading

         "query.rkt"
         "record.rkt")

(define q
  (~> (op:hash-join
       'candidate-id
       (lambda (id)
         (record (hasheq 'candidate-id 0
                         'candidate-name 1)
                 (vector id #f)))
       (~> (op:file-read "master_lookup.txt"          
                         line-reader)
           (op:extract-field 'raw 'record-type
                             (lambda~>
                              (substring 0 10)
                              string-trim))
           (op:filter (lambda~>
                       (record-field-value 'record-type)
                       (string=? "Candidate")))
           (op:extract-field 'raw 'candidate-id
                             (lambda~>
                              (substring 11 17)
                              string->number))
           (op:extract-field 'raw 'description
                             (lambda~>
                              (substring 17 67)
                              string-trim))
           (op:remove-field 'raw)
           (op:remove-field 'record-type)
           (op:rename-field 'description 'candidate-name))

       (~> (op:file-read "oak_mayor_20181117.txt"
                         line-reader)
           (op:extract-field 'raw 'rank
                             (lambda~>
                              (substring 33 36)
                              string->number))
           (op:extract-field 'raw 'voter
                             (lambda~>
                              (substring 7 16)
                              string->number))
           (op:extract-field 'raw 'candidate-id
                             (lambda (v)
                               (string->number
                                (substring v 36 43))))))
      (op:remove-field 'raw)    
      (op:split
       (lambda~>
        (op:filter (lambda~>
                    (record-field-value 'rank)
                    (= 1)))
        (op:rename-field 'candidate-name 'first-choice)
        (op:remove-field 'candidate-id)
        (op:remove-field 'rank))
       (lambda~>
        (op:filter (lambda~>
                    (record-field-value 'rank)
                    (= 2)))
        (op:rename-field 'candidate-name 'second-choice)
        (op:remove-field 'candidate-id)
        (op:remove-field 'rank))
       (lambda~>
        (op:filter (lambda~>
                    (record-field-value 'rank)
                    (= 3)))
        (op:rename-field 'candidate-name 'third-choice)
        (op:remove-field 'candidate-id)
        (op:remove-field 'rank)))      
      ((match-lambda
         [(list q1 q2 q3)
          (~> (op:hash-join
               'voter
               (lambda (id) (error 'hash-join "no default ~a" id))
               q1 q2)
              (op:hash-join
               'voter
               (lambda (id) (error 'hash-join "no default ~a" id))
               q3 _))]))))

(define printer
  (lambda~>
   (op:table-printer
               '(voter first-choice second-choice third-choice)
               (let ([f (lambda (a b c d)
                          (~a #:separator " | "                    
                              (~a #:width 10 a #:align 'right)
                              (~a #:width 20 b)
                              (~a #:width 20 c)
                              (~a #:width 20 d)))])
                 (case-lambda
                   [(fields) (apply f fields)]
                   [(fields . rest*) (apply f rest*)])))))

(~> q printer query-execute)
