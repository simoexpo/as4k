package org.simoexpo.as4k.model

final case class NonEmptyList[+T](head: T, tail: List[T]) {

  lazy val values: List[T] = head +: tail

}
