/** Initial load. Not edited */
(function(i,s,o,g,r,a,m){i['GoogleAnalyticsObject']=r;i[r]=i[r]||function(){
(i[r].q=i[r].q||[]).push(arguments);},i[r].l=1*new Date();a=s.createElement(o),
m=s.getElementsByTagName(o)[0];a.async=1;a.src=g;m.parentNode.insertBefore(a,m);
})(window,document,'script','https://www.google-analytics.com/analytics.js','ga');

/** This is used by the static sites to try and determine the correct GA ID. */
function setUpAgencyAnalyticsForStaticSite(isErrorPage)
{
	"use strict";
	
	function getAnalyticsId()
	{
		var DCCSDS_ID = "UA-16894847-1";
		var DATSIP_ID = "UA-32162959-2";
		
		switch(window.location.hostname)
		{
			case "www.datsip.qld.gov.au": 
			case "datsipintranet.root.internal":
			case "datsima.govnet.qld.gov.au":
				return DATSIP_ID;
			default: 
				return DCCSDS_ID;
		}
	}
	
	setUpAgencyAnalytics(getAnalyticsId(), isErrorPage);
}

/** This is used by standard pages */
function setUpAgencyAnalytics(id, isErrorPage)
{
	"use strict";

	ga('create', id, 'auto');

	if (isErrorPage)
	{
		var eRef = document.referrer; if (document.referrer) {var index = document.referrer.indexOf('//'); if (index > -1) {eRef = document.referrer.substring(index + 2);}}
		ga('send', 'pageview', '/error?url=' + window.location.pathname + (eRef.length > 0 ? '&amp;ref=' + eRef : ''));
	}
	else
	{
		ga('send', 'pageview');
	}
}

/**
 * To use externally use either of the following:
 * Track click: DOC_GA.trackClick({url});
 * Track event: DOC_GA.trackEvent({category}, {action}, {label});
 */
var DOC_GA = (function(window, document) 
{
	"use strict";

	/**
	 * Track a page view (standard page behaviour)
	 * @param {string} url - The URL to track.
	 */
	function trackClick(url)
	{
		if (ga !== undefined) 
		{
			ga('send', 'pageview', url);
		}
		else
		{
			throw new Error("window.ga is undefined. Please ensure Google Analytics is loaded.");
		}
	}
	
	
	/**
	 * Track a page view (standard page behaviour)
	 * @param {string} category - The group to record under, such as "Quicklinks" or "Video".
	 * @param {string} action - The action, such as "Click".
	 * @param {string} label - The identifier, such as an article title or link text.
	 */
	function trackEvent(category, action, label)
	{
		if (ga !== undefined) 
		{
			ga('send', 'event', category, action, label);
		}
		else
		{
			throw new Error("window.ga is undefined. Please ensure Google Analytics is loaded.");
		}
	}
	
	function setEventTrackingExternalLinks()
	{
		function getLink(el)
		{
			while (el.tagName !== 'A') 
			{
				el = el.parentNode;
			}
			
			var lnk = (el.pathname.charAt(0) === '/') ? el.pathname : '/' + el.pathname;
			if (el.search && el.pathname.indexOf(el.search) === -1) { lnk += el.search; }
			if (el.hostname !== location.hostname) { lnk = el.hostname + lnk; }

			return lnk;
		}
		
		function trackMailto(event) 
		{
			trackClick('/mailto/' + event.target.href.substring(7));
		}
		
		function trackExternalLinks(event) 
		{
			trackClick('/external/' + getLink(event.target));
		}
		
		function trackResourceLinks(event) 
		{
			trackClick(getLink(event.target));
		}
		
		// Track ALL external and document links
		var linkElements = document.getElementsByTagName('a');
		for (var i = 0; i < linkElements.length; i++) 
		{
			var linkElement = linkElements[i];
			if (linkElement.protocol === 'mailto:') 
			{
				linkElement.addEventListener('click', trackMailto);
			} 
			else if (linkElement.hostname !== location.hostname) 
			{
				linkElement.addEventListener('click', trackExternalLinks);
			} 
			else if (linkElement.pathname.indexOf(".") !== -1)
			{
				linkElement.addEventListener('click', trackResourceLinks);
			}
		}
	}

	/**
	 * Track feature clicks
	 */
	function setEventTrackingFeatures()
	{
		var slideElements = document.querySelectorAll(".slide");
		for (let i = 0; i < slideElements.length; i++)
		{
			var slideLink = slideElements[i].querySelector('a');
			if (slideLink)
			{
				DOC_GA.trackEvent('Features', 'click', slideLink.textContent + ' | ' + window.location.pathname);
			}
		}
	}

	/**
	 * Track footer clicks
	 */
	function setEventTrackingFooter()
	{
		var footerLinkElements = document.querySelectorAll("#footer a");
		for (let i = 0; i < footerLinkElements.length; i++)
		{
			var footerLinkElement = footerLinkElements[i];
			if (footerLinkElement)
			{
				DOC_GA.trackEvent('Footer', 'click', footerLinkElement.textContent + ' | ' + window.location.pathname);
			}
		}
	}

	/**
	 * Track tools clicks
	 */
	function setEventTrackingTools()
	{
		var linkElements = document.querySelectorAll("#tools a");
		for (let i = 0; i < linkElements.length; i++)
		{
			var linkElement = linkElements[i];
			if (linkElement)
			{
				DOC_GA.trackEvent('Tools', 'click', linkElement.textContent + ' | ' + window.location.pathname);
			}
		}
	}

	/**
	 * Track quicklink clicks
	 */
	function setEventTrackingQuicklinks()
	{
		var linkElements = document.querySelectorAll(".quicklinks a");
		for (let i = 0; i < linkElements.length; i++)
		{
			var linkElement = linkElements[i];
			if (linkElement)
			{
				DOC_GA.trackEvent('Quicklinks', 'click', linkElement.textContent + ' | ' + window.location.pathname);
			}
		}
	}

	/**
	 * Track form completion
	 */
	function setEventTrackingFormCompletion()
	{
		// Track form completion
		var inputElements = document.querySelectorAll("form.standard input");
		var totalInputs = inputElements.length;
		var currentlyCompletedInput = 0;
		var pageUrl = window.location.pathname;
		
		function trackFieldCompletion(event) 
		{
			currentlyCompletedInput++;
			var inputElement = event.target;
			if (inputElement.value !== "" && !inputElement.classList.contains("completed"))
			{
				DOC_GA.trackEvent("Form: " + pageUrl, "Completed", inputElement.getAttribute("name") + ": " + currentlyCompletedInput + "/" + totalInputs);
				inputElement.classList.add("completed");
			}
		}
		
		if (totalInputs !== 0)
		{
			for (let i = 0; i < inputElements.length; i++)
			{
				inputElements[i].addEventListener("blur", trackFieldCompletion);	
			}
		}
	}

	/** 
	 * Set default trackers
	 */
	function init()
	{
		
		setEventTrackingExternalLinks();

		setEventTrackingFeatures();

		setEventTrackingFooter();

		setEventTrackingTools();
		
		setEventTrackingQuicklinks();

		setEventTrackingFormCompletion();
	}
	
	
	if (document.attachEvent ? document.readyState === "complete" : document.readyState !== "loading")
	{
		init();
	} 
	else 
	{
		document.addEventListener('DOMContentLoaded', init);
	}
	
	
	/** Public methods */
	return {
		trackEvent: trackEvent,
		trackClick: trackClick
	};
	
}(window, document));

