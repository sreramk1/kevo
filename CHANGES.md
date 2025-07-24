# This documents the changes made to the original Kevo implementation, which includes changes to architectural assumptions and specification

## Major changes:

## Fine-grained/detailed changes: 

1. Package `transaction` under the folder `./pkg/transaction` contains the `Transaction` structure, defines the `active` variable to indicate if the specific transaction was active. This variable is accessed using atomic operations although there is no need for them, since all instances of its use are automatically serialized by a mutex lock.