// JavaScript Document
var mygallery=new fadeSlideShow({
	wrapperid: "fadeshow1", //ID of blank DIV on page to house Slideshow
	dimensions: [470, 220], //width/height of gallery in pixels. Should reflect dimensions of largest image
	imagearray: [
		
		["//www.spec-net.com.au/banners/200129_AWIS.jpg", "http://artisticwroughtiron.com.au/", "", "A Gorgeous Staircase Featuring AWIS Wrought Iron Components"],
		["//www.spec-net.com.au/banners/200129_Rollashield.jpg", "https://www.rollashieldshutters.com.au/bushfire-shutters/", "", "AS3959-2009 Compliant BAL FZ Bushfire Shutters from Rollashield"],
		["//www.spec-net.com.au/banners/200129_dPP-Hydronic-Heating.jpg", "https://www.spec-net.com.au/press/0120/dpp_290120/Elegant-Bathroom-Radiators-AGAVE-by-dPP-Hydronic-Heating", "", "Elegant Bathroom Radiators - AGAVE by dPP Hydronic Heating"],
		["//www.spec-net.com.au/banners/200129_RMS.jpg", "https://www.rmsmarble.com/vetrazzo-slabs/", "", "Handcrafted Recycled Glass Benchtops - Vetrazzo by RMS"],
		["//www.spec-net.com.au/banners/200129_Tornex-Door-Systems.jpg", "https://www.tornex.com.au/", "", "Single & Double Sliding Track Doors from Tornex Door Systems"],
		["//www.spec-net.com.au/banners/200129_GCP-Applied-Technologies.jpg", "https://gcpat.com.au/en-gb/news/blog", "", "GCP Preprufe Plus Seamless Protection of Underground Structures"],
		["//www.spec-net.com.au/banners/200205_Gorter-Hatches.jpg", "https://www.gortergroup.com/au/products/roof-hatches/rhtg-glazed.html", "", "Electrical Operated, Triple Glazed Roof Hatches by Gorter Hatches"],
		<!-- CONTRA -->
		
		<!-- SPEC-NET -->
		
		<!-- CAREVISION -->
        <!-- ["//www.spec-net.com.au/banners/car_tvthattalks.jpg", "https://www.carevision.com/the-tv-that-talks", "", "The CareVision TV is the Ultimate Independent Living Companion"], -->
	],
	displaymode: {type:'auto', pause:5000, cycles:0, wraparound:false},
	persist: false, //remember last viewed slide and recall within same session?
	fadeduration: 500, //transition duration (milliseconds)
	descreveal: "always",
	togglerid: ""
})
