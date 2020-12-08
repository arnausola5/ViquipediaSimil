package mapreduce

import scala.util.matching.Regex
import scala.xml.{Elem, XML}

object ViquipediaParse {

  // Fixem el fitxer xml que volem tractar a l'exemple
  val exampleFilename="viqui_files/32509.xml"

  // Definim una case class per a retornar diversos valor, el titol de la pàgina, el contingut i les referències trobades.
  // El contingut, s'ha de polir més? treure refs? stopwords?...
  case class ResultViquipediaParsing(titol: String, contingut: List[String], refs: List[String])

  def testParse= this.parseViquipediaFile(exampleFilename)

  def parseViquipediaFile(filename: String=this.exampleFilename) = {
    val xmlleg = new java.io.InputStreamReader(new java.io.FileInputStream(filename), "UTF-8")

    // Agafo el document XML i ja està internament estructurat per anar accedint als camps que volguem
    val xmllegg: Elem = XML.load(xmlleg)

    // obtinc el titol
    val titol = (xmllegg \\ "title").text

    val stopWords = ProcessListStrings.llegirFitxer("stopwordscatalanet.txt").split("\r\n").toList

    // obtinc el contingut de la pàgina
    val contingut = (xmllegg \\ "text").text
    val contingutNet = contingut.replaceAll("[^a-zA-Z0-9ÀàÁáÈèÉéÍíÒòÓóÚú ]", " ").toLowerCase.split(" +").filterNot(stopWords.contains(_)).toList

    // identifico referències
    val ref = new Regex("\\[\\[[^\\]]*\\]\\]")
    //println("La pagina es: " + titol)
    //println("i el contingut: ")
    //println(contingut)
    val refs = (ref findAllIn contingut).toList

    // elimino les que tenen :
    var filteredRefs = refs.filterNot(_.contains(':'))

    // caldrà eliminar-ne més?
    filteredRefs = filteredRefs.filterNot(_.contains('#'))

    //for (r <- refs) println(r)
    //println(refs.length)
    //println(filteredRefs.length)
    xmlleg.close()
    ResultViquipediaParsing(titol, contingutNet, filteredRefs)
  }
}