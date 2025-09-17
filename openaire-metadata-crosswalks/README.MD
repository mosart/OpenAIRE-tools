# Metadata Schema Crosswalk: from MODS XML to OpenAIRE CERIF XML

This repository contains a crosswalk from the MODS 3.6 metadata schema (as used in the Dutch EduStandaard application profile) to the OpenAIRE CERIF XML profile v1.2.

This is generated content using GPT-4o. See the full conversation here:  
👉 https://chatgpt.com/share/684fdce0-972c-8008-8df0-31c3de015626

## 🗂️ Contents

✅ [SSSOM mapping file](./mods_to_openaire_crosswalk.sssom.tsv)  
This file uses the [Simple Standard for Sharing Ontological Mappings (SSSOM)](https://sssom.readthedocs.io/en/latest/) format to describe conceptual equivalences between MODS elements and CERIF elements. Mappings are based on the EduStandaard 1.3 agreements for bibliographic metadata (2023-11-16).

📄 [XSLT transformation script](./mods_to_openaire_transform.xslt)  
This XSLT file transforms an input MODS XML document to a simplified OpenAIRE CERIF result object, covering core fields such as title, author names, DOI, and publication date.

📘 [Example MODS input](./example_input_mods.xml)  
A sample input MODS record with fields: title, abstract, DOI, author name, and date.

📗 [Example CERIF output](./example_output_openaire_cerif.xml)  
The result of applying the XSLT transformation to the example MODS record.

## 🔧 Usage

You can apply the XSLT transformation using any XSLT 2.0-compliant processor. Example using Saxon:

```bash
java -jar saxon.jar -s:example_mods.xml -xsl:mods_to_openaire_transform.xslt -o:output_cerif.xml
````

Or integrate the SSSOM mappings in semantic tools for schema alignment or linked data transformations.

## 📚 References

* MODS schema: [Library of Congress MODS 3.6](https://www.loc.gov/standards/mods/mods-outline-3-6.html)
* OpenAIRE CERIF Guidelines: [OpenAIRE CERIF v1.2](https://openaire-guidelines-for-cris-managers.readthedocs.io/en/v1.2.0/)
* EduStandaard MODS Profile: [EduStandaard Bibliografische Metadata v1.3, 2023-11-16](https://www.edustandaard.nl/standaard_afspraken/metadata-object-description-scheme-mods/metadata-object-description-scheme-versie-1-3/)

## 🌍 Contribution

This work can serve as input for the
👉 [EOSC Metadata Schema and Crosswalk Registry (MSCR)](https://mscr-release.2.rahtiapp.fi/)

Feel free to fork and extend this repository with more mappings, test data, or refined XSLT transformations.
