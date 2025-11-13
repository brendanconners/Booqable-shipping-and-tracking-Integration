fetch('https://quora-legal.booqable.com/api/4/barcodes')
.then(Response=>Response.json())
.then(data=> console.log(data))
.catch(error => console.error('error', error))