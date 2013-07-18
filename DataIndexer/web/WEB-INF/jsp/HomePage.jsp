<%@ include file="/WEB-INF/jsp/Include.jsp" %>

<html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en">
<head>
<title>Home Page </title>
<meta name="Author" content="John Fereira">
<meta name="Keywords" content="Home Page">
<meta name="Description" content="Home Page "> 

</head>
<body >  

<div class="content">
   <h3>Home Page </h3>
   <p>
   <form name="searchForm" method="post" action="showTriplesLocation.do" >
   <fieldset>
   <legend>Search for Triples Location</legend> 
   <label for="biburi"><strong>BibUri: </strong></label>
   <input type="text" name="biburi" id="biburi" size="80" value="<http://da-rdf.library.cornell.edu/individual/b3577819>"  /><br /><br />
   
   <p><strong>Or, you may just enter the bibid</p>
   <label for="bibid"><strong>BibId: </strong></label>
   <input type="text" name="bibid" id="bibid" size="20" value="3577819"  /><br /><br /> 
   
   <input type="hidden" name="redirect" value="false" />
   <p class="submit">
   <input type="Submit" value="Submit" />
   </p>
   </fieldset>
   </form>
    
</div> <!- end content div> 
</body>
</html>
