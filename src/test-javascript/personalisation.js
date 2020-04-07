//sets cookie
function createCookie(name,value,days) {
	var expires = "";
	if (days) {
		var date = new Date();
		date.setTime(date.getTime() + (days * 24 * 60 * 60 * 1000));
		expires = "; expires=" + date.toUTCString();
	}
	document.cookie = name + "=" + value + expires + "; domain=.csu.edu.au; path=/";
}

//reads cookie
function readCookie(name) {
    var nameEQ = name + "=";
    var ca = document.cookie.split(';');
    for(var i=0;i < ca.length;i++) {
        var c = ca[i];
        while (c.charAt(0)==' ') c = c.substring(1,c.length);
        if (c.indexOf(nameEQ) == 0) return c.substring(nameEQ.length,c.length);
    }
    return null;
}

//deletes cookie
function eraseCookie(name) {
    createCookie(name, "", -1);
}

function getSearchParams(k){
    var p={};
    location.search.replace(/[?&]+([^=&]+)=([^&]*)/gi,function(s,k,v){p[k]=v})
    return k?p[k]:p;
}


//heavy lifting
function initPersonalisation(ocb,level,location){
    //check for new session
    var from = document.referrer;
    //console.log('from: '+from);
    
    
    fakeCohort = getSearchParams("cohort");
    //console.log('cohort: '+fakeCohort);
    
    params = getSearchParams("cm");
    if (params == "false"){
        //$('#cohortModal').modal('destroy');
        var studyVisitor = {};
        if (fakeCohort == "int"){
            studyVisitor.geolocation = "International";
            $('#megaMenuContainer').load('https://study.csu.edu.au/_designs/nested-content/mega-menu-parent-list?site=3203128');
        } else {
            studyVisitor.geolocation = "Domestic";
        }
        createCookie("csuStudy", JSON.stringify(studyVisitor), 365);
        //console.log('no modal');
    } else {
        if (from.indexOf('study.csu.edu.au') == -1){
            //console.log('new session');
            var returner = readCookie("studyVisit");
            
            //console.log('returner: '+returner);
            
            //if second visit, show bottom sheet modal
            if (returner == "0"){
                //captures initial site visit if modal fails
                returner = "1";
                createCookie("studyVisit", returner, 365);
            } else if (returner == "1"){
                //first site visit if modal launches
                //console.log('first visit');
                returner ++;
                createCookie("studyVisit", returner, 365);
            } else if (returner == "2"){
                //second visit
                //console.log('launch bottom modal');
                returner ++
                createCookie('studyVisit', returner, 365);
                $('#bottomModal').modal('open');
            } else {
                //all other visits
                //console.log('not second visit');
            }
        } else {
            //console.log('existing session');
        }
        
        
        
        var studyVisitor = readCookie("csuStudy");  //read cookie
        //console.log('init studyVisitor: '+studyVisitor);
    
        if (studyVisitor != null) {  //cookie exists
            //console.log('have cookie already');
            studyVisitor = JSON.parse(studyVisitor);
            
            if (studyVisitor.geolocation == "International" && ocb != null){
                //console.log('load International mega-menu');
                $('#megaMenuContainer').load('https://study.csu.edu.au/_designs/nested-content/mega-menu-parent-list?site=3203128');
            }
    
            if (!ocb){
                locationCheck(studyVisitor.geolocation);
            }
        } else {    //no cookie
            //console.log('no cookie');

            if (ocb){
                //console.log('ocb');
            } else if (!ocb || !level || !location){
                $('#cohortModal').modal({
                    dismissible: false
                });
                //$('#bottomModal').modal('open');
                $('#cohortModal').modal('open');
            }

            
            studyVisitor = {};  //create new object
            studyVisitor.id = uuidv4(); //set unique id
            studyVisitor.firstVisited = new Date;   //set first visited date
            studyVisitor.visitedCourseCodes = []   //init course codes
            studyVisitor.studyLevel = "";   //init studyLevel
            studyVisitor.geolocation = "";  //init geolocation
        }
        
        studyVisitor.lastVisited = new Date;    //update last visited date
        
        if (ocb) {  //are we visiting an OCB
            studyVisitor.visitedCourseCodes.push(ocb);  //add pcode to string
            studyVisitor.geolocation = "Domestic";
        }
        
        //set study level if provided from landing page
        if (level) {
            studyVisitor.studyLevel = level;
        }
        
        //set geolocation if set from chip
        if (location) {
            studyVisitor.geolocation = location;
        }
        
        createCookie("csuStudy", JSON.stringify(studyVisitor), 365);    //create cookie
        //console.log('cookie created');
        //console.log(studyVisitor);

    }
        
}



//creates unique identifier
function uuidv4() {
    return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
        var r = Math.random() * 16 | 0, v = c == 'x' ? r : (r & 0x3 | 0x8);
        return v.toString(16);
    });
}

//sets geolocation based on modal identification
function closeModalSetCookie (geo){
    $('#cohortModal').modal('close');   //close modal
    
    //console.log('modal closes, check cookie');
    var returner = readCookie("studyVisit");
    //console.log('returner cookie val: '+returner);
    if (!returner){
        //console.log('returning cookie created');
        createCookie("studyVisit","1",365);
    } 
    
    initPersonalisation(null,null,geo);     //re-init cookie with location


    var page = location.href.replace(location.search, '');
    var campaign = getSearchParams("cid");
    var cohort;
    
    var domSite = [
        "https://study.csu.edu.au",
        "https://study.csu.edu.au/study-options",
        "https://study.csu.edu.au/courses",
        "https://study.csu.edu.au/life",
        "https://study.csu.edu.au/get-support",
        "https://study.csu.edu.au/apply",
        "https://study.csu.edu.au/contact-us",
        "https://study.csu.edu.au/online",
        "https://study.csu.edu.au/undergraduate",
        "https://study.csu.edu.au/postgraduate"
    ];
    
    var intSite = [
        "https://study.csu.edu.au/international",
        "https://study.csu.edu.au/international/study-options",
        "https://study.csu.edu.au/international/courses",
        "https://study.csu.edu.au/international/university-life",
        "https://study.csu.edu.au/international/get-support",
        "https://study.csu.edu.au/international/apply",
        "https://study.csu.edu.au/international/contact-us",
        "https://study.csu.edu.au/international/online",
        "https://study.csu.edu.au/international/undergraduate",
        "https://study.csu.edu.au/international/postgraduate"
    ];
    
    console.log('page:'+page);
    console.log('geo:'+geo);
    
    
    if (page.indexOf('/courses/all') == -1) {
        console.log('not on courses');
        

            if (page.indexOf('study.csu.edu.au/international/') == -1){
            //if (page.indexOf('/study/international') == -1){
                //console.log('on domestic site');
                if (geo == "International"){
                    var found = false;
                    for (i = 0; i < domSite.length; i++){
                        if (page == domSite[i]) {
                            if (campaign && campaign.length > 0){
                                var newURL = intSite[i]+'?cid='+campaign;
                            } else {
                                var newURL = intSite[i];
                            }
                            found = true;
                            $(location).attr('href', newURL);
                        }
                    }
                    if (!found){
                        window.location = "https://study.csu.edu.au/international";
                    }
                } 
            } else {
                //console.log('on international site');
                if (geo == "Domestic") {
                    var found = false;
                    for (j = 0; j < intSite.length; j++){
                        if (page == intSite[j]) {
                            if (campaign && campaign.length > 0){
                                var newURL = domSite[i]+'?cid='+campaign;
                                
                            } else {
                                var newURL = domSite[i];    
                            }
                            found = true;
                            $(location).attr('href', newURL);
                        }
                    }
                    if (!found){
                        window.location = "https://study.csu.edu.au/";
                    }
                } 
            }
        
    } else {
        
        //COURSES ALL
        console.log('on courses');
        if (page.indexOf('study.csu.edu.au/international/') == -1){
            if (geo == "International"){
                console.log('go to International version of this course page');
                var newUrl = [page.slice(0,25), 'international/', page.slice(25)].join('');
                console.log(newUrl);
                window.location = newUrl;
            } else {
                console.log('already on the Domestic version of this course page')
            }
        } else {
            if (geo == "Domestic"){
                console.log('go to Domestic version of this course page');
                var newUrl = page.substring(0,24) + page.substring(38, page.length);
                console.log(newUrl);
                window.location = newUrl;
            } else {
                console.log('already on the International version of this course page');
            }
        }
    }
    
    $("div[id$='cohort-display']").text(geo);
    
    var sv = geo.substr(0,3).toLowerCase();
    sv = sv + "-site";
    $("div[id$='cohort-switch'] ul li").removeClass('active');
    $("div[id$='cohort-switch'] ul").find("li[id$='${sv}']").addClass('active');

    var blinkCount = 0;
    
    var blinkTimer = setInterval(function(){
        $('#cohort-switch').toggleClass('blink');
        setTimeout(function(){
            $('#cohort-switch').toggleClass('blink');
        },500);
        //console.log('blink: '+blinkCount);
        if (blinkCount == 6){
            console.log('end');
            clearInterval(blinkTimer);
        } else {
            blinkCount++;
        }
    },1000);
    $('#cohort-switch').removeClass('blink');
    
}

function locationCheck(geo){
    var page = location.href.replace(location.search, '');
    var campaign = getSearchParams("cid");
    var cohort;
    if (page.indexOf('study.csu.edu.au/international') != -1){
        if (page.indexOf('study.csu.edu.au/international-students') != -1){
            cohort = "Domestic";
        } else {
            cohort = "International";
        }
    } else {
        cohort = "Domestic";
    }

    var domSite = [
        "https://study.csu.edu.au",
        "https://study.csu.edu.au/study-options",
        "https://study.csu.edu.au/courses",
        "https://study.csu.edu.au/life",
        "https://study.csu.edu.au/get-support",
        "https://study.csu.edu.au/apply",
        "https://study.csu.edu.au/contact-us",
        "https://study.csu.edu.au/online",
        "https://study.csu.edu.au/undergraduate",
        "https://study.csu.edu.au/postgraduate"
    ];
    
    var intSite = [
        "https://study.csu.edu.au/international",
        "https://study.csu.edu.au/international/study-options",
        "https://study.csu.edu.au/international/courses",
        "https://study.csu.edu.au/international/university-life",
        "https://study.csu.edu.au/international/get-support",
        "https://study.csu.edu.au/international/apply",
        "https://study.csu.edu.au/international/contact-us",
        "https://study.csu.edu.au/international/online",
        "https://study.csu.edu.au/international/undergraduate",
        "https://study.csu.edu.au/international/postgraduate"
    ];
    
    if (geo){
        if (geo == "Domestic"){
            if (cohort == "International"){
                for (i = 0; i < intSite.length; i++){
                    if (page == intSite[i]){
                        if (campaign && campaign.length > 0){
                            var newURL = domSite[i]+'?cid='+campaign;
                            console.log('newURL: '+newURL);
                            //window.location = newURL;
                            //window.location = domSite[i];
                            break;
                        } else {
                            var newURL = domSite[i];
                            console.log('newURL: '+newURL);
                            //window.location = newURL;
                            break;
                        }
                    }
                }
                $('#cohortModal').modal();
                $('#cohortModal').modal('open');
            } else {
                $('#cohortModal').modal();
                $('#cohortModal').modal('close');
            }
        } else if (geo == "International") { // geo == "International"
            if (cohort == "Domestic"){
                for (j = 0; j < domSite.length; j++){
                    if (page == domSite[j]){
                        if (campaign.length > 0){
                            var newURL = intSite[j]+'?cid='+campaign;
                            console.log('newURL: '+newURL);
                            //window.location = newURL;
                            //window.location = intSite[j];
                            break;
                        } else {
                            var newURL = intSite[j];
                            console.log('newURL: '+newURL);
                            //window.location = newURL;
                            break;
                        }
                    }
                }
                $('#cohortModal').modal();
                $('#cohortModal').modal('open');
            } else {
                $('#cohortModal').modal('close');
            }
        }
        
        $("div[id$='cohort-display']").text(geo);
        
        var sv = geo.substr(0,3).toLowerCase();
        sv = sv + "-site";
        $("div[id$='cohort-switch'] ul li").removeClass('active');
        $("div[id$='cohort-switch'] ul").find("li[id$='${sv}']").addClass('active');
    }
}


$(document).ready(function(){
    $('#hdr-modal').modal();
    $('#school-modal').modal();
    
    /*$('#bottomModal').modal({
        dismissible: false
    });*/
    $('#bottomModal').modal();
    
    //tracks clicks on landing pages
    $('#main-nav a.level').click(function(event){
        event.preventDefault();     //stops link from firing
        var level = $(this).data(level).level;  //grabs study level
        initPersonalisation(null,level,null);   //re-inits cookie with study level
        window.location = this.href;    //proceed to link as usual
    });
    
    //user selects Domestic from modal
    $('#modal-domestic').click(function(){
        console.log('geo = Domestic');
        closeModalSetCookie("Domestic");
    });
    
    //user selects International from modal
    $('#modal-international').click(function(){
        console.log('geo = International');
        closeModalSetCookie("International");
    });
});

$(window).on("load",function(){
    var studyVisitor = JSON.parse(readCookie("csuStudy"));
    $('#cohort-display').text(studyVisitor.geolocation);
    var sv = studyVisitor.geolocation.substr(0,3).toLowerCase();
    sv = sv + "-site";
    $("div[id$='cohort-switch'] ul").find(`li[id$='${sv}']`).addClass('active');
});