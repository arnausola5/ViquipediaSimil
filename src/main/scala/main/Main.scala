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
import scala.language.{implicitConversions, postfixOps}
import scala.io.StdIn.readLine

object primeraPart extends App {

  // main

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
    print("\nEntra la mida del ngrama (1,2 o 3): ")
    val midaNgrama = readLine().toInt

    val fitxer1 = ProcessListStrings.llegirFitxer("primeraPartPractica/" + fitxers(opcioFitxer1-1))
    val fitxer2 = ProcessListStrings.llegirFitxer("primeraPartPractica/" + fitxers(opcioFitxer2-1))

    val similitud = cosinesim(fitxer1, fitxer2, midaNgrama)
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

  def cosinesim(text1: String, text2: String, n: Int): Double = {
    val stopWords = ProcessListStrings.llegirFitxer("primeraPartPractica/english-stop.txt").split("\r\n").toList

    var freq1: List[(String, Int)] = null
    var freq2: List[(String, Int)] = null
    if(n == 1) {
      freq1 = nonstopfreq(text1, stopWords).sortWith(_._2 > _._2)
      freq2 = nonstopfreq(text2, stopWords).sortWith(_._2 > _._2)
    } else {
      freq1 = ngrames(text1, n).sortWith(_._2 > _._2)
      freq2 = ngrames(text2, n).sortWith(_._2 > _._2)
    }

    val freq1Normalitzat = freq1.map(m => (m._1, m._2.toFloat/freq1.take(1)(0)._2)).sortBy(_._1)
    val freq2Normalitzat = freq2.map(m => (m._1, m._2.toFloat/freq2.take(1)(0)._2)).sortBy(_._1)
    val freq2Map = freq2Normalitzat.toMap

    var producteScalar = 0.toFloat
    for((mot, freq) <- freq1Normalitzat) producteScalar = producteScalar + (freq  * freq2Map.getOrElse(mot, 0.toFloat))

    val sumFreq1 = Math.sqrt(freq1Normalitzat.map(m => m._2 * m._2).foldLeft(0.0)(_+_))
    val sumFreq2 = Math.sqrt(freq2Normalitzat.map(m => m._2 * m._2).foldLeft(0.0)(_+_))

    producteScalar / (sumFreq1 * sumFreq2)
  }
}

object segonaPart extends App {

  // main

  println("OPCIONS")
  println("--------------------------")
  println("1. Pàgines rellevants")
  println("2. Similitud")

  print("\nEntra el número de la opció a executar: ")
  val opcioFuncio = readLine().toInt
  print("\nEntra el nombre de mappers i reducers: ")
  val nMappersReducers = readLine().toInt
  print("Entra el nombre de fitxers a tractar: ")
  val nFitxers = readLine().toInt

  val systema: ActorSystem = ActorSystem("sistema")
  val fitxers = llistaFitxers(nFitxers);

  if(opcioFuncio == 1) {
    print("Entra el nombre de pàgines rellevants a mostrar: ")
    val nPaginesRellevants = readLine().toInt

    val paginesRellevants = systema.actorOf(Props(new MapReduce(prepararInputReferencies(fitxers), mappingReferencies, reducingReferencies, nMappersReducers, nMappersReducers)), name = "masterReferencies")

    implicit val timeout = Timeout(10000 seconds)
    var futurResultatPaginesRellevants = paginesRellevants ? mapreduce.MapReduceCompute()

    println("Esperant Pàgines rellevants")
    val resultatPaginesRellevants = Await.result(futurResultatPaginesRellevants, Duration.Inf).asInstanceOf[Map[String, Int]]

    println("Resultat obtingut")
    val resultatOrdenat = resultatPaginesRellevants.toSeq.sortWith(_._2 > _._2).take(nPaginesRellevants)
    for(pagina <- resultatOrdenat) println(pagina._1 + " apareix " + pagina._2 + " cops")
  }
  else {
    println("Entra el llindar de similitud: ")
    val llindarSimilitud = readLine().toFloat

    // Calculem el df per cada mot que apareix en algun dels fitxers
    val df = systema.actorOf(Props(new MapReduce(prepararInputDf(nFitxers), mappingDf, reducingDf, nMappersReducers, nMappersReducers)), name = "masterDf")

    implicit val timeout = Timeout(10000 seconds)
    var futurResultatDf = df ? mapreduce.MapReduceCompute()

    println("Esperant resultat df")
    val resultatDf = Await.result(futurResultatDf, Duration.Inf).asInstanceOf[Map[String, Int]]

    println("Resultat obtingut")
    // Calculem el valor d'idf per cada mot
    val idf: Map[String, Float] = resultatDf.map(m => (m._1, Math.log10(nFitxers.toFloat/m._2.toFloat).toFloat))

    // Calculem les pàgines que no es referencien mútuament
    val paginesNoRef = systema.actorOf(Props(new MapReduce(prepararInputNoRef(fitxers), mappingNoRef, reducingNoRef, nMappersReducers, nMappersReducers)), name = "masterNoRef")

    var futurResultatNoRef = paginesNoRef ? mapreduce.MapReduceCompute()

    println("Esperant resultat No es Referencien")
    val resultatNoRef = Await.result(futurResultatNoRef, Duration.Inf).asInstanceOf[Map[String, List[String]]]

    println("Resultat obtingut")

    systema.terminate()

    // Mirem tots els fitxers que tenim que no es referencien
    val fitxersDiferents = resultatNoRef.toList.map(t => t._1 :: t._2).flatten.toSet.toList

    println("S'han calculat els fitxers diferents que tenim")

    // Calculem el tf_idf per cada fitxer
    /*
          Obrirem només un cop cada fitxer.
          Per cada fitxer fet un set de tots els mots que té, i després calculem el seu tf per cada mot, contant el nombre d'ocurrències d'aquell mot en aquell fitxer.
          Per calcular el tf_idf per aquell mot en concret dins d'aquest fitxer, fa falta multiplicar el tf per l'idf d'aquell mot, com que si el mot existeix pel fitxer que estem tractan també existirà al idf,
            ja que hi ha tots els mots de tots els fitxers que estem tractant, busquem el valor d'idf al seu map i els multipliquem.
          Finalment per cada fitxer, guardem el seu tf_idf i també aprofitem per calcular el denominador del cosinesim, ja que mai variarà.
     */
    val tf_idf = (for(f <- fitxersDiferents) yield {  val valor = ViquipediaParse.parseViquipediaFile("viqui_files/" + f).contingut.groupBy(m => m).map(m => (m._1, m._2.length.toFloat * idf.getOrElse(m._1, 0.toFloat)))
                                                      (f, (valor, Math.sqrt(valor.map(m => m._2 * m._2).foldLeft(0.0)(_+_)).toFloat))}).toMap

    println("tf_idf calculat")

    val mapFitxers = fitxers.toMap
    var valorCosinesim = null
    /*
          Generem les parelles de fitxers que no es referencien mútuament i calculem el seu cosinesim, utilitzant els valors calculats prèviament al tf_idf.
          Finalment si superen el llindar de similitud, guardem la parella de fitxers amb el seu valor de similitud.
     */
    val similitud = for((f1, fitxers) <- resultatNoRef; f2 <- fitxers; valorCosinesim = cosinesim(tf_idf.getOrElse(f1, (Map(), 0.toFloat)), tf_idf.getOrElse(f2, (Map(), 0.toFloat))); if valorCosinesim > llindarSimilitud) yield ((mapFitxers.getOrElse(f1, "-"), mapFitxers.getOrElse(f2, "-")), valorCosinesim)

    for(s <- similitud) println("La similitud entre " + s._1._1 + " i " + s._1._2 + " és de %.3f", s._2)
  }

  // FUNCIONS

  // Agafem els n fitxers de la carpeta de "viqui_files", els retornem a una llista amb format de tupla, on el primer element és le nom del fitxer i el segon el seu títol
  def llistaFitxers(n: Int): List[(String, String)] =
    ProcessListStrings.getListOfFiles("viqui_files").take(n).map(f => (f.getName, ViquipediaParse.parseViquipediaFile(f.getPath).titol))

  // Per mirar els fitxers que es referencien, l'entrada que passem és el una llista de tuples, on el primer element és el nom del fitxer i el segon la llista de tots els altres fitxers però amb el seu títol
  def prepararInputReferencies(llistaFitxers: List[(String, String)]): List[(String, List[String])] =
    for ((nomFitxer, _) <- llistaFitxers) yield (nomFitxer, llistaFitxers.map(t => t._2))

  // Mirem si a la llista de referències del fitxer que arriba al mapping hi ha algun títol de tots els fitxers que estem considerant
  // Generem una tupla per cada títol de la llista, amb el nombre de cops que apareix a les referències del fitxer
  def mappingReferencies(fitxer: String, llistaTitolsFitxers: List[String]): List[(String, Int)] = {
    val referencies = ViquipediaParse.parseViquipediaFile("viqui_files/" + fitxer).refs
    for(titol <- llistaTitolsFitxers) yield (titol, referencies.count(_.contains(titol)))
  }

  // Per cada títol de fitxer, contem el nombre de cops que apareix com a referència en algun fitxer
  def reducingReferencies(titolFitxer: String, llistaReferencies: List[Int]): (String, Int) =
    (titolFitxer, llistaReferencies.sum)

  // Generem una llista de tuples, on el primer element és el path de cada fitxer i el segon una llista buida
  def prepararInputDf(n: Int): List[(String, List[String])] = {
    val llistaFitxers = ProcessListStrings.getListOfFiles("viqui_files").take(n)
    for(fitxer <- llistaFitxers) yield (fitxer.getPath, List[String]())
  }

  // Obrim cada fitxer, i per cada mot, generem un únic cop una tupla com a primer element el mot i com a segon un 1
  def mappingDf(pathFitxer: String, c: List[String]): List[(String, Int)] = {
    val setMots = ViquipediaParse.parseViquipediaFile(pathFitxer).contingut.toSet.toList
    for(mot <- setMots) yield (mot, 1)
  }

  // Sumem per cada mot les ocurrències
  def reducingDf(mot: String, ocurrencies: List[Int]): (String, Int) =
    (mot, ocurrencies.sum)

  // La funció per l'input del MapReduce dels que no es referencien mútuament
  // És una llista de tuples, on el primer element és el nom del fitxer i el segon una llista de tuples, on el primer element és el nom del fitxer i el segon el títol
  def prepararInputNoRef(llistaFitxers: List[(String, String)]): List[(String, List[(String, String)])] =
    for ((nomFitxer, _) <- llistaFitxers) yield (nomFitxer, llistaFitxers)

  // Generem tuples amb els dos noms de dos fitxers que no es referencien mútuament, aquestes tuples les ordenem de petit a gran, ja que d'aquesta manera al reducing podrem treure els repetits
  // en cas de que dos fitxers no es referencin en cap de les dos direccions
  def mappingNoRef(nomFitxer: String, llistaTitolsFitxers: List[(String, String)]): List[(String, String)] = {
    val fitxer = ViquipediaParse.parseViquipediaFile("viqui_files/" + nomFitxer)
    val referencies = fitxer.refs
    val titolFitxer = fitxer.titol
    for((nomf, titol) <- llistaTitolsFitxers if titol != titolFitxer && !referencies.exists(_.contains(titol))) yield { if(nomFitxer < nomf) (nomFitxer, nomf) else (nomf, nomFitxer) }
  }

  // Eliminem els repetits en cas de ser-hi
  def reducingNoRef(nomFitxer: String, nomFitxers: List[String]): (String, List[String]) = {
    (nomFitxer, nomFitxers.toSet.toList)
  }

  // Per calcular el cosinesim mirem quin dels dos fitxers és mes gran, ja que agafant el més petit serà més ràpid, perquè el nombre d'iteracions serà menor
  def cosinesim(v1: (Map[String, Float], Float),  v2: (Map[String, Float], Float)): Float = {
    var producteEscalar = 0.0
    if(v1._1.size < v2._1.size)
      for((mot, valor) <- v1._1) producteEscalar = producteEscalar + (valor * v2._1.getOrElse(mot, 0.toFloat))
    else
      for((mot, valor) <- v2._1) producteEscalar = producteEscalar + (valor * v1._1.getOrElse(mot, 0.toFloat))

    (producteEscalar / (v1._2 * v2._2)).toFloat
  }
}


