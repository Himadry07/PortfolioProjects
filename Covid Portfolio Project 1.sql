SELECT *
FROM PortfolioProject..CovidDeaths
Order By 3,4  --jodio eitar kono use ekhane hoi nai just process maintain korar jonno rakhsi. Karon amar gulate already data ordered way te chilo

--SELECT *
--FROM PortfolioProject..CovidVaccinations
--Order By 3,4

--Select Data we are going to be using - 1

SELECT Location, date, total_cases, new_cases,total_deaths, population
FROM PortfolioProject..CovidDeaths
Order By 1,2

-- Looking at total cases vs total deaths -2
-- Shows likelihood of dying if you contrct covid in your country
SELECT Location, date, total_cases, total_deaths, (total_deaths * 100) / total_cases as DeathPercentage
FROM PortfolioProject..CovidDeaths
WHERE Location LIKE '%states%' 
ORDER BY 1, 2

--SELECT Location, date, total_cases, total_deaths, (total_deaths * 100) / total_cases) as DeathPercentage
--FROM PortfolioProject..CovidDeaths
--WHERE Location LIKE '%desh%' -- In this process, we are using the sub-string to find a location here which is a country named Bangladesh. Here we can see that in the column name upper and lower cases don't matter at all.
--ORDER BY 1, 2

-- Looking at total cases vs population - 3
-- Shows the percentage of population got covid 
SELECT Location, date, total_cases, population,(total_cases * 100) / population as InfectionRate
FROM PortfolioProject..CovidDeaths
WHERE Location LIKE '%desh%' 
ORDER BY 1, 2

--Looking at countries with highest infectionRate Copared to the Population - 4

SELECT Location, population, MAX(total_cases) as HighestInfectionCount, Max((total_cases * 100) / population ) as HighestInfectionRate
FROM PortfolioProject..CovidDeaths   --Aggregated fucntion chara baki gula Group by statement e ullekh thakte hobe otherwise error show korbe
GROUP BY Location, population
Order By 4 Desc

--Showing Countries With Highest death count per population - 5

SELECT Location, MAX(cast(Total_deaths as BIGINT)) as TotalDeathCount
FROM PortfolioProject..CovidDeaths   --Ekhane problem hoche total_deaths er data type varchar ache kintu amra oreder kortesi integer akare tai output
WHERE continent is not null
GROUP BY Location                    -- Jhamela purno hoche otoyeb amaderke eita properly use korte hobe Cast use kore convert kore numeric ba interger e.
Order By TotalDeathCount Desc

-- Let's break thing out by continent
-- Showing death counts with highest death count per population
SELECT continent, MAX(cast(Total_deaths as BIGINT)) as TotalDeathCount
FROM PortfolioProject..CovidDeaths   --Ekhane problem hoche total_deaths er data type varchar ache kintu amra oreder kortesi integer akare tai output
WHERE continent is not null
GROUP BY continent                   -- Jhamela purno hoche otoyeb amaderke eita properly use korte hobe Cast use kore convert kore numeric ba interger e.
Order By TotalDeathCount Desc

-- Global Numbers

SELECT date, SUM(new_cases) as total_cases, SUM(cast(new_deaths as int)) as total_deaths, SUM(cast(new_deaths as int))*100/SUM(new_cases) as DeathPercentage
FROM PortfolioProject..CovidDeaths
--WHERE Location LIKE '%states%' 
WHERE continent is not null
GROUP BY date   -- It will gave a sum of new cases across the world on a certain date as it is not filtered by any continent or location 
ORDER BY 1, 2


SELECT SUM(new_cases) as total_cases, SUM(cast(new_deaths as int)) as total_deaths, SUM(cast(new_deaths as int))*100/SUM(new_cases) as DeathPercentage
FROM PortfolioProject..CovidDeaths
--WHERE Location LIKE '%states%' 
WHERE continent is not null
--GROUP BY date   -- It will gave a sum of new cases across the world on a certain date as it is not filtered by any continent or location 
ORDER BY 1, 2


--Looking at total Population vs Vaccinations

-- jodi eki column duita table e thake tahole amaderke specify korte hobe kon table theke amara eta insert korte chachi
--ekhane partition by use korsi karon nahoi sum onoboroto cholte thakto ar eita obviously amra chaina, tai ekta location por por jate abar new kore counting start kore.
--Ar over by use kora hoyeche sum ta jeno rolling basis e hoi orthat ekta certain date obviously location por por jate sum ber hoi
-- Total cases already given ekta column but amaderke dewa ba thakle amra je nijera ber korte pari tar ekta example ekhane dekhano holo
SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations,--or SUM(cast(vac.new_vaccinations as int))
SUM(Convert(int,vac.new_vaccinations)) OVER (Partition By dea.location Order By dea.location, dea.date) as RollingPeopleVaccinated
FROM PortfolioProject..CovidDeaths dea
Join PortfolioProject..CovidVaccinations vac
	on dea.location = vac.location
	and dea.date  = vac.date
WHERE dea.continent is not null
Order By 2,3

--ekhon amra chachi total population er under e kotojon vaccinated hoyeche tar ekta percentage ber korbo. kintu amra ekhane shorashori ta korte partesina orthat
--Rolling people vaccinated use korte partesina tai ekhane amaderke CTE ba Temp  table use korte hobe. Amra ekhon temp table use korbo

--USE CTE


With PopvsVac (Continent, Location, Date, Population, New_Vaccinations, RollingPeopleVaccinated)
as
(
Select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations
, SUM(CONVERT(int,vac.new_vaccinations)) OVER (Partition by dea.Location Order by dea.location, dea.Date) as RollingPeopleVaccinated
--, (RollingPeopleVaccinated/population)*100
From PortfolioProject..CovidDeaths dea
Join PortfolioProject..CovidVaccinations vac
	On dea.location = vac.location
	and dea.date = vac.date
where dea.continent is not null 
--order by 2,3
)

--Select* , (RollingPeopleVaccinated/Population)*100 as VaccinationPercentage
--From PopvsVac
--Group By Location

Select Location, MAX((RollingPeopleVaccinated/Population)*100) as VaccinationPercentage
From PopvsVac
Group By Location

-- Temp Table
Drop table if exists #PercentPopulationVaccinated 
CREATE table #PercentPopulationVaccinated 
(
Continent nvarchar(255),
Location nvarchar(255),
Date datetime,
Population numeric,
New_vaccinations numeric,
RollingPeopleVaccinated numeric
)

Insert into #PercentPopulationVaccinated 
Select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations
, SUM(CONVERT(int,vac.new_vaccinations)) OVER (Partition by dea.Location Order by dea.location, dea.Date) as RollingPeopleVaccinated
--, (RollingPeopleVaccinated/population)*100
From PortfolioProject..CovidDeaths dea
Join PortfolioProject..CovidVaccinations vac
	On dea.location = vac.location
	and dea.date = vac.date
where dea.continent is not null 

Select *, (RollingPeopleVaccinated/Population)*100
From #PercentPopulationVaccinated


--CTE(Cmmon Table Expression)s are only available within the context of a single query. They are not stored permanently, and they cannot be accessed by other queries. Temporary tables, on the other hand, can be created and accessed by any query in the current session

-- Creating view to store data for later visiualization
-- Creating View to store data for later visualizations

Create View PercentPopulationVaccinated as
Select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations
, SUM(CONVERT(int,vac.new_vaccinations)) OVER (Partition by dea.Location Order by dea.location, dea.Date) as RollingPeopleVaccinated
--, (RollingPeopleVaccinated/population)*100
From PortfolioProject..CovidDeaths dea
Join PortfolioProject..CovidVaccinations vac
	On dea.location = vac.location
	and dea.date = vac.date
where dea.continent is not null 

SELECT *
FROM PercentPopulationVaccinated





