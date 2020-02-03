from django.shortcuts import render
from webapp.forms import userForms
from webapp.InstagramAnalytics import instagramAnalytics

def index(request):
    print("done")
    if request.method == "POST":
        instaPy = instagramAnalytics()
        instaPy.extract(request.POST.get("username"," "),request.POST.get("pass"," "))
        #print(request.POST.get("username"," "))
        #print(request.POST.get("pass"," "))
    return render(request, 'webapp/home.html')

def view_details(request):
    print("done")

