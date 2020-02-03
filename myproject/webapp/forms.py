from django import forms

class userForms(forms.Form):
    username = forms.CharField()
    password = forms.CharField()
