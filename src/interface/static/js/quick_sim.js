function handle_team_pics() {
    alert('asdf');
    teams = document.getElementsByClassName("team_pic")
    for (var i = 0; i < teams.length; i++) {
        teams[i].addEventListener("click", function () {
            teams[i].className = "selected_team";
        });
    }
}
