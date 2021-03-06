"""viewer URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/3.0/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.conf import settings
from django.contrib import admin
from django.urls import include, path
from viewer import views, stats

urlpatterns = [
    path('admin/', admin.site.urls),
]

#print(admin.site.urls)

if settings.DEBUG:
    import debug_toolbar
    urlpatterns = [
        path('__debug__/', include(debug_toolbar.urls)),
    ] + urlpatterns

urlpatterns = [
        path('', views.index),
        path('search/', views.host_search),
        path('search/host/', views.host_search),
        path('search/control/', views.control_search),
        path('search/distance/', views.distance_search),
        path('search/recent/', views.recent_search),
        path('search/function/', views.function_search),
        path('statistics/by-control/', stats.by_control_stats),
    ] + urlpatterns
