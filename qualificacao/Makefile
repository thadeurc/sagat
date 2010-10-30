qualificacao.pdf: qualificacao.tex referencias.bib
	latex qualificacao
	bibtex qualificacao
	latex qualificacao
	latex qualificacao
	dvips -Pcmz -Pamz -t A4size qualificacao.dvi -o qualificacao.ps
	open qualificacao.ps
clean:
	rm -rf qualificacao.aux qualificacao.toc qualificacao.bbl qualificacao.blg qualificacao.log qualificacao.pdf qualificacao.dvi qualificacao.ps
