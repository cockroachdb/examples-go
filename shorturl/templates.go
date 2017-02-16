package main

import (
	"html/template"
	"log"
	"net/http"
)

var (
	templateView     *template.Template
	templateCreate   *template.Template
	templateViewMine *template.Template
)

const (
	templateViewText = `
<!DOCTYPE html>
<html>
	<head>
		<meta charset="UTF-8">
		<title>Shorty: {{.ShortURL}}</title>
	</head>
	<body>
  	<h3>Shorty URL: {{.ShortURL}}</h3>
	  <table border=0>
			<tr>
			  <td>ShortURL</td>
				<td>{{.ShortURL}}</td>
			</tr>
			<tr>
			  <td>LongURL</td>
				<td>{{.LongURL}}</td>
			</tr>
			<tr>
			  <td>Public</td>
				<td>{{.Public}}</td>
			</tr>
			<tr>
			  <td>Custom</td>
				<td>{{.Custom}}</td>
			</tr>
			<tr>
			  <td>Added</td>
				<td>{{.DateAdded.Format "Mon Jan 2 15:04:05"}}</td>
			</tr>
			<tr>
	      <td>Owner</td>
				<td>{{.AddedBy}}</td>
			</tr>
		</table>
		<br>
	</body>
</html>`

	templateCreateText = `
<!DOCTYPE html>
<html>
	<head>
		<meta charset="UTF-8">
		<title>Shorty</title>
	</head>
	<body>
	  <h3>Create URL:</h3>
	  <form action="/?mode=create" method="post">
	  <table border=0>
		  <tr>
		      <td>LongURL</td>
					<td><input type="text" name="url" size=100></td>
			</tr>
			<tr>
			    <td>Pick automatically</td>
					<td><input type="checkbox" id="custom" checked></td>
			</tr>
	    <script type="text/javascript">
			  document.getElementById("custom").onchange = function() {
						shortBox = document.getElementById("short")
						shortBox.disabled=this.checked
						shortBox.value = ""
				}
				this.checked = true
		</script>
			<tr>
	        <td>ShortURL</td>
					<td><input type="text" name="short" id="short" disabled></td>
			</tr>
			<tr>
		      <td>Public</td>
					<td><input type="checkbox" name="public"></td>
			</tr>
			<tr>
		      <td><input type="submit" value="Submit"></td>
			</tr>
		</table>
		</form>
		<br>
	</body>
</html>`

	templateViewMineText = `
<!DOCTYPE html>
<html>
	<head>
		<meta charset="UTF-8">
		<title>Shorty: my URLs</title>
	</head>
	<body>
		<h3>My URLs:</h3>
	  <table border=0>
			<tr>
			  <td><b>ShortURL</b></td>
			  <td><b>LongURL</b></td>
			  <td><b>Public</b></td>
			  <td><b>Added</b></td>
			</tr>

			{{range .}}
			<tr>
				<td>{{.ShortURL}}</td>
				<td>{{.LongURL}}</td>
				<td>{{.Public}}</td>
				<td>{{.DateAdded.Format "Mon Jan 2 15:04:05"}}</td>
			</tr>
			{{end}}
		</table>
	  <br>
	</body>
</html>`
)

func init() {
	var err error
	if templateView, err = template.New("view").Parse(templateViewText); err != nil {
		log.Fatalf("error parsing view template: %v", err)
	}

	if templateCreate, err = template.New("create").Parse(templateCreateText); err != nil {
		log.Fatalf("error parsing create template: %v", err)
	}

	if templateViewMine, err = template.New("view mine").Parse(templateViewMineText); err != nil {
		log.Fatalf("error parsing view-mine template: %v", err)
	}
}

func viewShorty(rw http.ResponseWriter, shorty *Shorty) error {
	return templateView.Execute(rw, shorty)
}

func showCreateForm(rw http.ResponseWriter) error {
	return templateCreate.Execute(rw, nil)
}

func viewMyShortys(rw http.ResponseWriter, shortys []*Shorty) error {
	return templateViewMine.Execute(rw, shortys)
}
