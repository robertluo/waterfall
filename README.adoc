# Waterfall

[![CI](https://github.com/robertluo/waterfall/actions/workflows/main.yml/badge.svg)](https://github.com/robertluo/waterfall/actions/workflows/main.yml)
[![Clojars Project](https://img.shields.io/clojars/v/io.github.robertluo/waterfall.svg)](https://clojars.org/io.github.robertluo/waterfall)
[![cljdoc badge](https://cljdoc.org/badge/io.github.robertluo/waterfall)](https://cljdoc.org/d/io.github.robertluo/waterfall)

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
 - Minimum necessary dependencies. 
 - Clojure version SERDES, in `robertluo.waterfall.shape` namespace. Underlying, give Kafka API byte array SERDES. 
 - Optional features require optional dependencies. For example, if you want to use nippy as SERDES, you put it in your classpath.

## Development experience

Because all functions have their schemas incorporated, you can get the best development experience if dependency `metosin/malli` is in your classpath.

Put `(malli.dev/start!)` in your `user.clj` will [enable clj-kondo](https://github.com/metosin/malli/blob/master/docs/function-schemas.md#tldr) to use the schemas when editing.

## Single API

Powered by [fun-map](https://github.com/robertluo/fun-map), you can use one single API for accessing a Kafka cluster without any further knowledge:

 - [`kafka-cluster`](https://cljdoc.org/d/io.github.robertluo/waterfall/CURRENT/api/robertluo.waterfall#kafka-cluster)

You can see an example in [this easy example notebook](notebook/easy.clj).

## API namespaces

  | namespace | Description |
  | -- | -- |
  | robertluo.waterfall | main API |
  | robertluo.waterfall.shape | optional Clojure data shape transformation |

## Files Description

  | Filename | Description |
  | -- | -- |
  | deps.edn  | Clojure tools.deps configuration |
  | tests.edn | kaocha test runner configuration |
  | build.clj | Clojure tools.build building script |
  | notebook  | [Clerk](https://github.com/nextjournal/clerk) notebooks to demostrate usage, use `clojure -M:dev:notebook` to use them |

## Unlicense

2022, Robertluo