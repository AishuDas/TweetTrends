from django.conf.urls import url
from . import views  # import from current dir.

#call index function views
urlpatterns = [
	url(r'^$',views.index, name='index'),
	url(r'^tweet/$',views.gettweet, name='gettweet'),
	]
