# Waterfall

[![CI](https://github.com/robertluo/waterfall/actions/workflows/main.yml/badge.svg)](https://github.com/robertluo/waterfall/actions/workflows/main.yml)

## Rational

Try to use Apache Kafka clients in idiomatic Clojure.

Kafka has Java/Scala clients API, and programmers have no problem calling them directly in Clojure, so we do not need to "wrap" Java API.

However, from a Clojure programmer's perspective, direct call Kafka Java API does not feel very good:

 - It is complex, with many interfaces, classes, and methods, which is not a good experience for a dynamic language.
 - It looks more like a framework or even frameworks. Users have to have a lot of background knowledge and implementation details.
 - Clojure programmers could use transducers to deal with data and do not need to use Kafka streams. 

Hence, Waterfall is an attempt at a minimalist library with additional optional batteries.

 - Only two core functions: `producer` and `consumer`.
 - Relying on the excellent [Manifold library](https://github.com/clj-commons/manifold), `producer`, and `consumer` is just the implementation of the Manifold stream.
 - Clojure version SERDES, in `robertluo.waterfall.shape` namespace. Underlying, give Kafka API byte array SERDES. 

## API namespaces

  | namespace | Description |
  | -- | -- |
  | robertluo.waterfall | main API |
  | robertluo.waterfall.shape | optional Clojure data shape transformation |

## Files Description

  | Filename | Description |
  | -- | -- |
  | deps.edn | Clojure tools.deps configuration |
  | tests.edn | kaocha test runner configuration |
  | build.clj | Clojure tools.build building script |

## Copyright

Luo Tian @2022