<?xml version="1.0" encoding="UTF-8"?>

<!DOCTYPE xsl:stylesheet [
<!ENTITY marcrdf 'http://marcrdf.library.cornell.edu/canonical/0.1/' >
<!ENTITY rdf "http://www.w3.org/1999/02/22-rdf-syntax-ns#" >
<!ENTITY rdfs 'http://www.w3.org/2000/01/rdf-schema#' >
<!ENTITY ind 'http://da-rdf.library.cornell.edu/individual' >
]>

<xsl:stylesheet version="2.0"
	xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
	xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
	xmlns:core="http://vivoweb.org/ontology/core#"
	xmlns:foaf="http://xmlns.com/foaf/0.1/"
	xmlns:score='http://vivoweb.org/ontology/score#'
	xmlns:bibo='http://purl.org/ontology/bibo/'
	xmlns:rdfs='http://www.w3.org/2000/01/rdf-schema#'
	xmlns:ufVivo='http://vivo.ufl.edu/ontology/vivo-ufl/'
	xmlns:marcrdf='http://marcrdf.library.cornell.edu/canonical/0.1/'
	xmlns:marcslim='http://www.loc.gov/MARC21/slim'>
<xsl:output method="text" /> 

<xsl:template name="string-replace-all">
  <xsl:param name="text"/>
  <xsl:param name="replace"/>
  <xsl:param name="by"/>
  <xsl:choose>
    <xsl:when test="contains($text,$replace)">
      <xsl:value-of select="substring-before($text,$replace)"/>
      <xsl:value-of select="$by"/>
      <xsl:call-template name="string-replace-all">
        <xsl:with-param name="text" select="substring-after($text,$replace)"/>
        <xsl:with-param name="replace" select="$replace"/>
        <xsl:with-param name="by" select="$by"/>
      </xsl:call-template>
    </xsl:when>
    <xsl:otherwise>
      <xsl:value-of select="$text"/>
    </xsl:otherwise>
  </xsl:choose>
</xsl:template>

<xsl:variable name="escape">\</xsl:variable>
<xsl:variable name="escaped_escape">\\</xsl:variable>
<xsl:variable name="quote">"</xsl:variable>
<xsl:variable name="escaped_quote">\"</xsl:variable>

<xsl:template name="normalize-text">
  <xsl:param name="text" />

  <xsl:call-template name="string-replace-all">
    <xsl:with-param name="text">
      <xsl:call-template name="string-replace-all">
        <xsl:with-param name="text" select="$text"/>
        <xsl:with-param name="replace" select="$escape"/>
        <xsl:with-param name="by" select="$escaped_escape"/>
      </xsl:call-template>
    </xsl:with-param>
    <xsl:with-param name="replace" select="$quote"/>
    <xsl:with-param name="by" select="$escaped_quote"/>
  </xsl:call-template></xsl:template>


<xsl:template match="marcslim:record">
  &lt;&ind;/b<xsl:value-of select="marcslim:controlfield[@tag='001']"/>&gt; &lt;&rdf;type&gt; &lt;&marcrdf;BibliographicRecord&gt; .
  &lt;&ind;/b<xsl:value-of select="marcslim:controlfield[@tag='001']"/>&gt; &lt;&rdfs;label&gt; &quot;<xsl:value-of select="marcslim:controlfield[@tag='001']"/>&quot; .
<xsl:apply-templates/>
</xsl:template>

<xsl:template match="marcslim:leader">
  &lt;&ind;/b<xsl:value-of select="../marcslim:controlfield[@tag='001']"/>&gt; &lt;&marcrdf;leader&gt; &quot;<xsl:value-of select="." />&quot; .
</xsl:template>

<xsl:template match="marcslim:controlfield">
  &lt;&ind;/b<xsl:value-of select="../marcslim:controlfield[@tag='001']"/>&gt; &lt;&marcrdf;hasField&gt; &lt;&ind;/b<xsl:value-of select="../marcslim:controlfield[@tag='001']"/>_<xsl:number/>&gt; .
  &lt;&ind;/b<xsl:value-of select="../marcslim:controlfield[@tag='001']"/>_<xsl:number/>&gt; &lt;&rdf;type&gt; &lt;&marcrdf;ControlField&gt; .
  &lt;&ind;/b<xsl:value-of select="../marcslim:controlfield[@tag='001']"/>_<xsl:number/>&gt; &lt;&marcrdf;tag&gt; &quot;<xsl:value-of select="@tag"/>&quot; .
  &lt;&ind;/b<xsl:value-of select="../marcslim:controlfield[@tag='001']"/>_<xsl:number/>&gt; &lt;&marcrdf;value&gt; &quot;<xsl:call-template name="normalize-text"><xsl:with-param name="text" select="."/></xsl:call-template>&quot; .
</xsl:template>

<xsl:template match="marcslim:datafield">
  &lt;&ind;/b<xsl:value-of select="../marcslim:controlfield[@tag='001']"/>&gt; &lt;&marcrdf;hasField&gt; &lt;&ind;/b<xsl:value-of select="../marcslim:controlfield[@tag='001']"/>_<xsl:value-of select="count(./preceding-sibling::*)"/>&gt; .
  &lt;&ind;/b<xsl:value-of select="../marcslim:controlfield[@tag='001']"/>_<xsl:value-of select="count(./preceding-sibling::*)"/>&gt; &lt;&rdf;type&gt; &lt;&marcrdf;DataField&gt; .
  &lt;&ind;/b<xsl:value-of select="../marcslim:controlfield[@tag='001']"/>_<xsl:value-of select="count(./preceding-sibling::*)"/>&gt; &lt;&marcrdf;tag&gt; &quot;<xsl:value-of select="@tag"/>&quot; .
  &lt;&ind;/b<xsl:value-of select="../marcslim:controlfield[@tag='001']"/>_<xsl:value-of select="count(./preceding-sibling::*)"/>&gt; &lt;&marcrdf;ind1&gt; &quot;<xsl:value-of select="@ind1"/>&quot; .
  &lt;&ind;/b<xsl:value-of select="../marcslim:controlfield[@tag='001']"/>_<xsl:value-of select="count(./preceding-sibling::*)"/>&gt; &lt;&marcrdf;ind2&gt; &quot;<xsl:value-of select="@ind2"/>&quot; .
<xsl:apply-templates/>
</xsl:template>

<xsl:template match="marcslim:subfield">
  &lt;&ind;/b<xsl:value-of select="../../marcslim:controlfield[@tag='001']"/>_<xsl:value-of select="count(parent::*/preceding-sibling::*)"/>&gt; &lt;&marcrdf;hasSubfield&gt;  &lt;&ind;/b<xsl:value-of select="../../marcslim:controlfield[@tag='001']"/>_<xsl:value-of select="count(parent::*/preceding-sibling::*)"/>_<xsl:number/>&gt; .
  &lt;&ind;/b<xsl:value-of select="../../marcslim:controlfield[@tag='001']"/>_<xsl:value-of select="count(parent::*/preceding-sibling::*)"/>_<xsl:number/>&gt; &lt;&rdf;type&gt; &lt;&marcrdf;Subfield&gt; .
  &lt;&ind;/b<xsl:value-of select="../../marcslim:controlfield[@tag='001']"/>_<xsl:value-of select="count(parent::*/preceding-sibling::*)"/>_<xsl:number/>&gt; &lt;&marcrdf;code&gt; &quot;<xsl:value-of select="@code"/>&quot; .
  &lt;&ind;/b<xsl:value-of select="../../marcslim:controlfield[@tag='001']"/>_<xsl:value-of select="count(parent::*/preceding-sibling::*)"/>_<xsl:number/>&gt; &lt;&marcrdf;value&gt; &quot;<xsl:call-template name="normalize-text"><xsl:with-param name="text" select="normalize-space(.)"/></xsl:call-template>&quot; .
</xsl:template>

<xsl:template match="holdings">
</xsl:template>

</xsl:stylesheet>
