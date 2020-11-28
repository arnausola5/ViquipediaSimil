package main

import java.io.File

import akka.actor.{ActorSystem, PoisonPill, Props}
import akka.pattern.ask
import akka.util.Timeout
import mapreduce._

import scala.collection.immutable.ListMap
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

// Tenim dos objectes executables
// - tractaxml utilitza un "protoParser" per la viquipèdia i
// exampleMapreduce usa el MapReduce

object primeraPart extends App {

  val aliceWonderland = ProcessListStrings.llegirFitxer("primeraPartPractica/pg11.txt")
  /* val frequenciaParaules = freq(aliceWonderland)
  println("Num de paraules: " + frequenciaParaules.foldLeft(0)(_+_._2))
  println("Diferents: " + frequenciaParaules.length)
  println(frequenciaParaules.sortWith(_._2>_._2).take(10))

  val stopWords = ProcessListStrings.llegirFitxer("primeraPartPractica/english-stop.txt").split("\r\n").toList
  println(nonstopfreq(aliceWonderland, stopWords).sortWith(_._2>_._2).take(10)) */

  // paraulafreqfreq(aliceWonderland)

  // val ngrams = ngrames(aliceWonderland, 3)
  // println(ngrams.sortWith(_._2>_._2).take(10))

  val throughtLookingGlass = ProcessListStrings.llegirFitxer("primeraPartPractica/pg12.txt")

  println(cosinesim(aliceWonderland, throughtLookingGlass))


  def freq(text: String):List[(String, Int)] =
    text.replaceAll("[^a-zA-Z ]", " ").split(" +").groupBy(m => m.toLowerCase()).map(m => (m._1, m._2.length)).toList

  def nonstopfreq(text: String, stopWords: List[String]):List[(String, Int)] =
    text.replaceAll("[^a-zA-Z ]", " ").toLowerCase().split(" +").filterNot(stopWords.contains(_)).groupBy(m => m).map(m => (m._1, m._2.length)).toList

  def paraulafreqfreq(text: String) = {
    val frequencies = freq(text).groupBy(_._2).map(n => (n._1, n._2.length)).toList.sortBy(_._1)
    for(freq <- frequencies.take(10)) println(freq._2 + " paraules apareixen " + freq._1 + " vegades")
    for(freq <- frequencies.drop(frequencies.length-5)) println(freq._2 + " paraules apareixen " + freq._1 + " vegades")
  }

  def ngrames(text: String, n: Int): List[(String, Int)] =
    text.replaceAll("[^a-zA-Z ]", " ").split(" +").sliding(n).map(n => n.mkString(" ")).toList.groupBy(m => m.toLowerCase()).map(m => (m._1, m._2.length)).toList

  def cosinesim(text1: String, text2: String): Double = {
    val stopWords = ProcessListStrings.llegirFitxer("primeraPartPractica/english-stop.txt").split("\r\n").toList

    val freq1 = nonstopfreq(text1, stopWords).sortWith(_._2>_._2)
    val freq1Normalitzat = freq1.map(m => (m._1, m._2.toFloat/freq1.take(1)(0)._2)).sortBy(_._1)
    val freq2 = nonstopfreq(text2, stopWords).sortWith(_._2>_._2)
    val freq2Normalitzat = freq2.map(m => (m._1, m._2.toFloat/freq2.take(1)(0)._2)).sortBy(_._1)
    val freq2Map = freq2Normalitzat.toMap

    var producteScalar = 0.0
    for((mot, freq) <- freq1Normalitzat) producteScalar = producteScalar + (freq  * freq2Map.getOrElse(mot, 0.toFloat))

    val sumFreq1 = Math.sqrt(freq1Normalitzat.map(m => m._2 * m._2).foldLeft(0.0)(_+_))
    val sumFreq2 = Math.sqrt(freq2Normalitzat.map(m => m._2 * m._2).foldLeft(0.0)(_+_))

    producteScalar / (sumFreq1 * sumFreq2)
  }
}

object segonaPart extends App {

  def prepararInput(n: Int): List[(String, List[String])] = {
    val llistaFitxers = ProcessListStrings.getListOfFiles("viqui_files").take(n).map(f => (f.getName, ViquipediaParse.parseViquipediaFile(f.getPath).titol))

    for ((nomFitxer, _) <- llistaFitxers) yield (nomFitxer, llistaFitxers.map(t => t._2))
  }

  // f té una referència de fitxer
  def mappingReferencies(fitxer: String, llistaTitolsFitxers: List[String]): List[(String, Int)] = {
    val referencies = ViquipediaParse.parseViquipediaFile("viqui_files/" + fitxer).refs
    for(titol <- llistaTitolsFitxers) yield (titol, referencies.count(_.contains(titol)))
  }

  def reducingReferencies(fitxer: String, llistaReferencies: List[Int]): (String, Int) =
    (fitxer, llistaReferencies.sum)


  // main
  val systema: ActorSystem = ActorSystem("sistema")
  val paginesRellevants = systema.actorOf(Props(new MapReduce(prepararInput(5000),mappingReferencies,reducingReferencies, 10, 10)), name = "masterReferencies")

  implicit val timeout = Timeout(10000 seconds)
  var futureresResultPaginesRellevants = paginesRellevants ? mapreduce.MapReduceCompute()

  println("Awaiting")
  val paginesRellevantsResult:Map[String,Int] = Await.result(futureresResultPaginesRellevants,Duration.Inf).asInstanceOf[Map[String,Int]]

  println("Results Obtained")

  for(p <- paginesRellevantsResult) println(p)

  systema.terminate()
}

object fitxers extends App{
  ProcessListStrings.mostrarTextDirectori("primeraPartPractica")
}

object tractaxml extends App {

  val parseResult= ViquipediaParse.parseViquipediaFile()

  parseResult match {
    case ViquipediaParse.ResultViquipediaParsing(t,c,r) =>
      println("TITOL: "+ t)
      println("CONTINGUT: ")
      println(c)
      println("REFERENCIES: ")
      println(r)
  }
}

object exampleMapreduce extends App {

  val nmappers = 1
  val nreducers = 1
  val f1 = new java.io.File("f1")
  val f2 = new java.io.File("f2")
  val f3 = new java.io.File("f3")
  val f4 = new java.io.File("f4")
  val f5 = new java.io.File("f5")
  val f6 = new java.io.File("f6")
  val f7 = new java.io.File("f7")
  val f8 = new java.io.File("f8")

  val fitxers: List[(File, List[String])] = List(
    (f1, List("hola", "adeu", "per", "palotes", "hola","hola", "adeu", "pericos", "pal", "pal", "pal")),
    (f2, List("hola", "adeu", "pericos", "pal", "pal", "pal")),
    (f3, List("que", "tal", "anem", "be")),
    (f4, List("be", "tal", "pericos", "pal")),
    (f5, List("doncs", "si", "doncs", "quin", "pal", "doncs")),
    (f6, List("quin", "hola", "vols", "dir")),
    (f7, List("hola", "no", "pas", "adeu")),
    (f8, List("ahh", "molt", "be", "adeu")))


  val compres: List[(String,List[(String,Double, String)])] = List(
    ("bonpeu",List(("pep", 10.5, "1/09/20"), ("pep", 13.5, "2/09/20"), ("joan", 30.3, "2/09/20"), ("marti", 1.5, "2/09/20"), ("pep", 10.5, "3/09/20"))),
    ("sordi", List(("pep", 13.5, "4/09/20"), ("joan", 30.3, "3/09/20"), ("marti", 1.5, "1/09/20"), ("pep", 7.1, "5/09/20"), ("pep", 11.9, "6/09/20"))),
    ("canbravo", List(("joan", 40.4, "5/09/20"), ("marti", 100.5, "5/09/20"), ("pep", 10.5, "7/09/20"), ("pep", 13.5, "8/09/20"), ("joan", 30.3, "7/09/20"), ("marti", 1.5, "6/09/20"))),
    ("maldi", List(("pepa", 10.5, "3/09/20"), ("pepa", 13.5, "4/09/20"), ("joan", 30.3, "8/09/20"), ("marti", 0.5, "8/09/20"), ("pep", 72.1, "9/09/20"), ("mateu", 9.9, "4/09/20"), ("mateu", 40.4, "5/09/20"), ("mateu", 100.5, "6/09/20")))
  )

  // Creem el sistema d'actors
  val systema: ActorSystem = ActorSystem("sistema")

  // funcions per poder fer un word count
  def mappingWC(file:File, words:List[String]) :List[(String, Int)] =
        for (word <- words) yield (word, 1)


  def reducingWC(word:String, nums:List[Int]):(String,Int) =
        (word, nums.sum)


  println("Creem l'actor MapReduce per fer el wordCount")
  val wordcount = systema.actorOf(Props(new MapReduce(fitxers,mappingWC,reducingWC, 4, 4)), name = "mastercount")

  // Els Futures necessiten que se'ls passi un temps d'espera, un pel future i un per esperar la resposta.
  // La idea és esperar un temps limitat per tal que el codi no es quedés penjat ja que si us fixeu preguntar
  // i esperar denota sincronització. En el nostre cas, al saber que el codi no pot avançar fins que tinguem
  // el resultat del MapReduce, posem un temps llarg (100000s) al preguntar i una Duration.Inf a l'esperar la resposta.

  // Enviem un missatge com a pregunta (? enlloc de !) per tal que inicii l'execució del MapReduce del wordcount.
  //var futureresutltwordcount = wordcount.ask(mapreduce.MapReduceCompute())(100000 seconds)

  implicit val timeout = Timeout(10000 seconds) // L'implicit permet fixar el timeout per a la pregunta que enviem al wordcount. És obligagori.
  var futureresutltwordcount = wordcount ? mapreduce.MapReduceCompute()

  println("Awaiting")
  // En acabar el MapReduce ens envia un missatge amb el resultat
  val wordCountResult:Map[String,Int] = Await.result(futureresutltwordcount,Duration.Inf).asInstanceOf[Map[String,Int]]


  println("Results Obtained")
  for(v<-wordCountResult) println(v)

  // Fem el shutdown del actor system
  println("shutdown")
  systema.terminate()
  println("ended shutdown")
  // com tancar el sistema d'actors.

  /*
  EXERCICIS:

  Useu el MapReduce per saber quant ha gastat cada persona.

  Useu el MapReduce per saber qui ha fet la compra més cara a cada supermercat

  Useu el MapReduce per saber quant s'ha gastat cada dia a cada supermercat.
   */


  println("tot enviat, esperant... a veure si triga en PACO")
}



