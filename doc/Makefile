.PHONY: all pdf xelatex clean

# Zalecane: `make` (latexmk wykonuje xelatex, biber i powtórne przebiegi automatycznie; konfiguracja w latexmkrc)
all: pdf

pdf:
	latexmk
	@! grep -E "Citation .* undefined|There were undefined references" main.log || (echo "BŁĄD: niezdefiniowane cytowania lub odwołania" && false)

xelatex:
	xelatex -interaction=nonstopmode -halt-on-error main
	biber main
	xelatex -interaction=nonstopmode -halt-on-error main
	xelatex -interaction=nonstopmode -halt-on-error main

fast:
	xelatex -interaction=nonstopmode -halt-on-error main

clean:
	-latexmk -C >/dev/null 2>&1
	-rm -f main.bbl-SAVE-ERROR
	-rm -f *.acn *.acr *.alg *.app *.aux *.bbl *.bcf *.blg *.fdb_latexmk *.fls *.glg *.glo *.gls *.ist *.lof *.log *.lot *.out *.pdf *.run.xml *.slo *.sls *.synctex.gz *.toc *.xdv
