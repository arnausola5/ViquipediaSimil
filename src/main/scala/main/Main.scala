package main

import java.io.File
import akka.actor.{ActorSystem, PoisonPill, Props}
import akka.pattern.ask
import akka.util.Timeout
import mapreduce.ViquipediaParse.parseViquipediaFile
import mapreduce.{ViquipediaParse, _}

import scala.collection.immutable
import scala.collection.immutable.ListMap
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.io.StdIn.readLine

object primeraPart extends App {

  println("OPCIONS")
  println("--------------------------")
  println("1. Freqüències de paraules")
  println("2. Sense stop-words")
  println("3. Distribució de paraules")
  println("4. Ngrames")
  println("5. Vector space model")

  print("\nEntra el número de la opció a executar: ")
  val opcioFuncio = readLine().toInt

  val fitxers = List("pg11.txt", "pg11-net.txt", "pg12.txt", "pg12-net.txt", "pg74.txt", "pg74-net.txt", "pg2500.txt", "pg2500-net.txt")

  println("\nFITXERS")
  println("--------------------------")
  println("1. pg11.txt")
  println("2. pg11-net.txt")
  println("3. pg12.txt")
  println("4. pg12-net.txt")
  println("5. pg74.txt")
  println("6. pg-74.txt")
  println("7. pg2500.txt")
  println("8. pg2500-net.txt")

  if(opcioFuncio >= 1 && opcioFuncio <= 4) {
    print("\nEntra el número del fitxer a tractar: ")
    val opcioFitxer = readLine().toInt
    val fitxer = ProcessListStrings.llegirFitxer("primeraPartPractica/" + fitxers(opcioFitxer - 1))

    if (opcioFuncio == 1 || opcioFuncio == 2) {
      var frequenciaParaules: List[(String, Int)] = null

      if (opcioFuncio == 1)
        frequenciaParaules = freq(fitxer)
      else {
        val stopWords = ProcessListStrings.llegirFitxer("primeraPartPractica/english-stop.txt").split("\r\n").toList
        frequenciaParaules = nonstopfreq(fitxer, stopWords)
      }

      val numParaules = frequenciaParaules.foldLeft(0)(_ + _._2)

      println("\nNum de Paraules: " + numParaules + "\t Diferents: " + frequenciaParaules.length)
      printf("%-13s %-15s %-15s\n", "Paraules", "tOcurrències", "Freqüència")
      println("----------------------------------------")
      val solucio = frequenciaParaules.sortWith(_._2 > _._2).take(10)
      for (mot <- solucio) printf("%-13s %-15s %.2f\n", mot._1, mot._2, mot._2.toFloat / numParaules * 100)
    }
    else if (opcioFuncio == 3)
      paraulafreqfreq(fitxer)

    else if (opcioFuncio == 4) {
      print("\nEntra la mida dels ngrames: ")
      val n = readLine().toInt

      val frequenciaNgrames = ngrames(fitxer, n)
      val solucio = frequenciaNgrames.sortWith(_._2 > _._2).take(10)
      for (ngrama <- solucio) printf("\n%-25s %s", ngrama._1, ngrama._2)
    }
  }
  else if(opcioFuncio == 5) {
    print("\nEntra el número del 1er fitxer a comparar: ")
    val opcioFitxer1 = readLine().toInt
    print("Entra el número del 2n fitxer a comparar: ")
    val opcioFitxer2 = readLine().toInt

    val fitxer1 = ProcessListStrings.llegirFitxer("primeraPartPractica/" + fitxers(opcioFitxer1-1))
    val fitxer2 = ProcessListStrings.llegirFitxer("primeraPartPractica/" + fitxers(opcioFitxer2-1))

    val similitud = cosinesim(fitxer1, fitxer2)
    printf("\nLa similitud entre " + fitxers(opcioFitxer1-1) + " i " + fitxers(opcioFitxer2-1) + " és de %.3f", similitud)
  }

  // FUNCIONS

  def freq(text: String):List[(String, Int)] =
    text.replaceAll("[^a-zA-Z ]", " ").split(" +").groupBy(m => m.toLowerCase()).map(m => (m._1, m._2.length)).toList

  def nonstopfreq(text: String, stopWords: List[String]):List[(String, Int)] =
    text.replaceAll("[^a-zA-Z ]", " ").toLowerCase().split(" +").filterNot(stopWords.contains(_)).groupBy(m => m).map(m => (m._1, m._2.length)).toList

  def paraulafreqfreq(text: String) = {
    val frequencies = freq(text).groupBy(_._2).map(n => (n._1, n._2.length)).toList.sortBy(_._1)
    println("\nLes 10 freqüències més freqüents:")
    for(freq <- frequencies.take(10)) println(freq._2 + " paraules apareixen " + freq._1 + " vegades")
    println("\nLes 5 freqüències menys freqüents:")
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

  def llistaFitxers(n:Int): List[(String, String)] =
    ProcessListStrings.getListOfFiles("viqui_files").take(n).map(f => (f.getName, ViquipediaParse.parseViquipediaFile(f.getPath).titol))

  def prepararInputReferencies(llistaFitxers: List[(String, String)]): List[(String, List[String])] =
    for ((nomFitxer, _) <- llistaFitxers) yield (nomFitxer, llistaFitxers.map(t => t._2))

  // f té una referència de fitxer
  def mappingReferencies(fitxer: String, llistaTitolsFitxers: List[String]): List[(String, Int)] = {
    val referencies = ViquipediaParse.parseViquipediaFile("viqui_files/" + fitxer).refs
    for(titol <- llistaTitolsFitxers) yield (titol, referencies.count(_.contains(titol)))
  }

  def reducingReferencies(fitxer: String, llistaReferencies: List[Int]): (String, Int) =
    (fitxer, llistaReferencies.sum)

  def prepararInputDf(n: Int): List[(String, List[String])] = {
    val llistaFitxers = ProcessListStrings.getListOfFiles("viqui_files").take(n)
    for(fitxer <- llistaFitxers) yield (fitxer.getPath, List[String]())
  }

  def mappingDf(fitxer: String, c: List[String]): List[(String, Int)] = {
    val set = ViquipediaParse.parseViquipediaFile(fitxer).contingut.toSet.toList
    for(mot <- set) yield (mot, 1)
  }

  def reducingDf(mot: String, aparences: List[Int]): (String, Int) =
    (mot, aparences.sum)

  def prepararInputNoRef(llistaFitxers: List[(String, String)]): List[(String, List[(String, String)])] =
    for ((nomFitxer, _) <- llistaFitxers) yield (nomFitxer, llistaFitxers)

  def mappingNoRef(fitxer: String, llistaTitolsFitxers: List[(String, String)]): List[(String, String)] = {
    val f = ViquipediaParse.parseViquipediaFile("viqui_files/" + fitxer)
    val referencies = f.refs
    val titolFitxer = f.titol
    for((nomf, titol) <- llistaTitolsFitxers if titol != titolFitxer && !referencies.exists(_.contains(titol))) yield { if(fitxer < nomf) (fitxer, nomf) else (nomf, fitxer) }
  }

  def reducingNoRef(fitxer1: String, fitxers2: List[String]): (String, List[String]) = {
    (fitxer1, fitxers2.toSet.toList)
  }

  // main
  val systema: ActorSystem = ActorSystem("sistema")

  val fitxers = llistaFitxers(5000);

  /* val paginesRellevants = systema.actorOf(Props(new MapReduce(prepararInputReferencies(fitxers),mappingReferencies,reducingReferencies, 10, 10)), name = "masterReferencies")

  implicit val timeout = Timeout(10000 seconds)
  var futureresResultPaginesRellevants = paginesRellevants ? mapreduce.MapReduceCompute()

  println("Awaiting")
  val paginesRellevantsResult:Map[String,Int] = Await.result(futureresResultPaginesRellevants,Duration.Inf).asInstanceOf[Map[String,Int]]

  println("Results Obtained")
  for(p <- paginesRellevantsResult) println(p) */

  val nDocs = 5000
  val df = systema.actorOf(Props(new MapReduce(prepararInputDf(nDocs), mappingDf, reducingDf, 10, 10)), name = "masterDf")

  implicit val timeout = Timeout(10000 seconds)
  var futurDf = df ? mapreduce.MapReduceCompute()

  println("Awaiting")
  val resultatDf:Map[String, Int] = Await.result(futurDf, Duration.Inf).asInstanceOf[Map[String, Int]]

  println("Results Obtained")
  val idf:Map[String, Float] = resultatDf.map(m => (m._1, Math.log10(nDocs.toFloat/m._2.toFloat).toFloat))

  // for(m <- idf) println(m)

  val paginesNoRef = systema.actorOf(Props(new MapReduce(prepararInputNoRef(fitxers),mappingNoRef,reducingNoRef, 10, 10)), name = "masterNoRef")

  // implicit val timeout = Timeout(10000 seconds)
  var futurNoRef = paginesNoRef ? mapreduce.MapReduceCompute()

  println("Awaiting")
  val noRef:Map[String,List[String]] = Await.result(futurNoRef,Duration.Inf).asInstanceOf[Map[String,List[String]]]

  println("Results Obtained")
  var sum = 0
  for(p <- noRef) sum = sum + p._2.size

  println(sum)

  systema.terminate()

  val fitxersDiferents = noRef.toList.map(t => t._1 :: t._2).flatten.toSet.toList

  println("Fitxers diferents fets")

  val tf_idf = (for(f <- fitxersDiferents) yield { val valor = ViquipediaParse.parseViquipediaFile("viqui_files/" + f).contingut.groupBy(m => m).map(m => (m._1, m._2.length.toFloat * idf.getOrElse(m._1, 0.toFloat)))
                                                    (f, (valor, Math.sqrt(valor.map(m => m._2 * m._2).foldLeft(0.0)(_+_)).toFloat))}).toMap

  println("Calcul tf_idf fet")

  // for(p <- tf_idf) println(p)

  def cosinesim(v1: (Map[String, Float], Float),  v2: (Map[String, Float], Float)): Float = {
    var producteEscalar = 0.0
    if(v1._1.size < v2._1.size)
      for((mot, valor) <- v1._1) producteEscalar = producteEscalar + (valor * v2._1.getOrElse(mot, 0.toFloat))
    else
      for((mot, valor) <- v2._1) producteEscalar = producteEscalar + (valor * v1._1.getOrElse(mot, 0.toFloat))

    (producteEscalar / (v1._2 * v2._2)).toFloat
  }

  val mapFitxers = fitxers.toMap
  var vCos = null;
  var setFitxers = null;
  val similitud = for((f1, fitxers) <- noRef; f2 <- fitxers; vCos = cosinesim(tf_idf.getOrElse(f1, (Map(), 0.toFloat)), tf_idf.getOrElse(f2, (Map(), 0.toFloat))); if vCos > 0.5) yield ((mapFitxers.getOrElse(f1, "-"), mapFitxers.getOrElse(f2, "-")), vCos)

  for(s <- similitud) println(s)
}


