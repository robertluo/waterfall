# Waterfall

An attempt for Kafka in ideomatical Clojure.

Libraries like Kinsky try to provide wrappers for Kafka java client, which is not very useful.

Ketu is nice, however, 

 * The implementation is not my favorite style

## Tasks

 - [ ] Test environment
 - [x] Core data structures
 - [x] Convert core data into Kafka config
 - [ ] Producer
  - [x] confirmed by `kafka-console-consumer.sh`
 - [ ] Consumer
 - [ ] Implement EventBus

## Files Description

  | Filename | Description |
  | -- | -- |
  | deps.edn | Clojure tools.deps configuration |
  | tests.edn | kaocha test runner configuration |

## Copyright

Luo Tian @2022