# Compile
javac -d bin src/Main.java

if ($LASTEXITCODE -eq 0) {
    Write-Host "Compilation successful. Starting Redis server on port 6379..." -ForegroundColor Green
    java -cp bin Main $args
} else {
    Write-Host "Compilation failed." -ForegroundColor Red
}
