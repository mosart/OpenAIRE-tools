<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="2.0"
    xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
    xmlns:mods="http://www.loc.gov/mods/v3"
    xmlns:cerif="https://www.openaire.eu/cerif-profile/1.2/"
    exclude-result-prefixes="mods">

  <xsl:output method="xml" indent="yes"/>

  <!-- Match the root MODS element -->
  <xsl:template match="/mods:mods">
    <cerif:result>
      <cerif:Title>
        <xsl:value-of select="mods:titleInfo/mods:title"/>
      </cerif:Title>
      <cerif:Abstract>
        <xsl:value-of select="mods:abstract"/>
      </cerif:Abstract>
      <cerif:DOI>
        <xsl:value-of select="mods:identifier[@type='doi']"/>
      </cerif:DOI>
      <cerif:PublicationDate>
        <xsl:value-of select="mods:originInfo/mods:dateIssued"/>
      </cerif:PublicationDate>
      <cerif:Author>
        <cerif:GivenName>
          <xsl:value-of select="mods:name/mods:namePart[@type='given']"/>
        </cerif:GivenName>
        <cerif:FamilyName>
          <xsl:value-of select="mods:name/mods:namePart[@type='family']"/>
        </cerif:FamilyName>
      </cerif:Author>
    </cerif:result>
  </xsl:template>

</xsl:stylesheet>
