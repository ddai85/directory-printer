# directory-printer

- Manage Directory Lists at https://people.planningcenteronline.com/lists
- Manage app at https://console.cloud.google.com
- Clear thumbnail cache by:
  - Make sure logged into correct google account
  - Select project "directory-export-pdf"
  - Open "Hamburger" menu top left
  - Select Storage >> Browser
  - Open "directory-export-pdf.appspot.com"
  - Delete all contents within-- should be a directory with a number eg "217317"
- Directions to deploy app:
  - Navigate to code directory in terminal
  - enter command: "gcloud app deploy app.yaml"
  - enter command: "gcloud app deploy app_directory.yaml"
- Directions to take down app (2 methods):
  1. Navigate to google cloud console in browser, find the app versions and delete
  2. - enter command: "gcloud app services delete default"
     - enter command: "gcloud app services delete hinson"
