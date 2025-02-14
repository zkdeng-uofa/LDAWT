<!-- Improved compatibility of back to top link: See: https://github.com/othneildrew/Best-README-Template/pull/73 -->
<a name="readme-top"></a>

<!-- PROJECT LOGO -->
<!-- <br />
<div align="center">
  <a href="https://github.com/github_username/repo_name">
    <img src="images/logo.png" alt="Logo" width="80" height="80">
  </a> -->

<h2 align="center">Large Dataset Acquisition Workflow Template (LDAWT)</h2>

  <p align="center">
    Python tool to download large datasets using a collaborative distributed parallel workflow. 
    <br />
    <!--<a href="https://github.com/github_username/repo_name"><strong>Explore the docs »</strong></a>-->
    <br />
    <!--
    <a href="https://github.com/github_username/repo_name">View Demo</a>
    ·
    <a href="https://github.com/github_username/repo_name/issues">Report Bug</a>
    ·
    <a href="https://github.com/github_username/repo_name/issues">Request Feature</a>-->
  </p>
</div>



<!-- TABLE OF CONTENTS -->
<details>
  <summary>Table of Contents</summary>
  <ol>
    <li>
      <a href="#about-the-project">About The Project</a>
      <ul>
        <!--<li><a href="#built-with">Built With</a></li>-->
      </ul>
    </li>
    <li>
      <a href="#getting-started">Getting Started</a>
      <ul>
        <li><a href="#prerequisites">Prerequisites</a></li>
        <li><a href="#installation">Installation</a></li>
      </ul>
    </li>
    <li><a href="#usage">Usage</a></li>
    <!--<li><a href="#contributing">Contributing</a></li>-->
    <li><a href="#license">License</a></li>
    <li><a href="#contact">Contact</a></li>
    <li><a href="#acknowledgments">Acknowledgments</a></li>
  </ol>
</details>



<!-- ABOUT THE PROJECT -->
## About The Project

The Large Dataset Acquisition Workflow Template (LDAWT), is a robust
and user-friendly pipeline that uses parallelism to retrieve datasets for machine learning applications. At a high level, LDAWT uses a multi-machine approach to download large datasets in a distributed parallel manner to achieve data acquisition speeds significantly faster than a single machine approach.

LDAWT uses a manager-worker paradigm, a the central manager machine efficiently partitions datasets into appropriate subsets, assigns them to worker machines for concurrent download jobs, then consolidates that final dataset onto the manager machine. 

<!-- GETTING STARTED -->
## Getting Started

This project uses the <a href="https://ccl.cse.nd.edu/software/taskvine/">Taskvine</a> framework to create easy to use and reproducible collabortive workflows. 

### Prerequisites

Conda is an open source package distributer and manager. This project uses conda to install packages.

* <a href="https://docs.conda.io/projects/conda/en/latest/user-guide/install/index.html">Conda Installation Page</a> 

### Installation

1. Clone the repository.
   ``` sh
   git clone https://github.com/zkdeng-uofa/LDAWT
   ```
2. Move into the LDAWT folder.
   ``` sh
   cd /path/to/LDAWT
   ```
3. Create a conda environment the requisite packages
   ``` sh 
   conda env create -f environment.yaml
   ```

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<!-- USAGE EXAMPLES -->
## Usage
<!-- Simple Example -->
### A Simple Example
Note both machines must have the LDAWT environment created/activated.

On your manager machine. 
1. Decide on a port of communification to use to communicate with worker machines. (default = 9124)
2. Create a LDAWT config file and store it within the files/config_files folder.
3. Find the communication ip address of your manager machine.
4. Run the command
  ``` sh 
  python bin/TaskvineLDAWT.py --config_file path/to/your/config/file
  ```

On your worker machine
1. Run the vine_worker command to join as a worker
  ``` sh 
  vine_worker manager/ip/address /manager/port/number
  ```
  (example)
  ``` sh
  vine_worker 127.0.0.1 9124
  ```
<p align="right">(<a href="#readme-top">back to top</a>)</p>

### Jupyter Notebook Examples

More detailed examples can be found within the jupyter folder.
<!-- LICENSE -->
## License

Please consult zkdeng@arizona.edu for use.
<p align="right">(<a href="#readme-top">back to top</a>)</p>

<!-- CONTACT -->
## Contact

Zi Deng - zkdeng@arizona.edu

Project Link: [https://github.com/zkdeng-uofa/LDAWT](https://github.com/zkdeng-uofa/LDAWT)

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<!-- ACKNOWLEDGMENTS -->
## Acknowledgments

* University of Arizona Data Science Institute
* Nirav Merchant - nirav@arizona.edu
<p align="right">(<a href="#readme-top">back to top</a>)</p>

<!-- MARKDOWN LINKS & IMAGES -->
<!-- https://www.markdownguide.org/basic-syntax/#reference-style-links -->
[contributors-shield]: https://img.shields.io/github/contributors/github_username/repo_name.svg?style=for-the-badge
[contributors-url]: https://github.com/github_username/repo_name/graphs/contributors
[forks-shield]: https://img.shields.io/github/forks/github_username/repo_name.svg?style=for-the-badge
[forks-url]: https://github.com/github_username/repo_name/network/members
[stars-shield]: https://img.shields.io/github/stars/github_username/repo_name.svg?style=for-the-badge
[stars-url]: https://github.com/github_username/repo_name/stargazers
[issues-shield]: https://img.shields.io/github/issues/github_username/repo_name.svg?style=for-the-badge
[issues-url]: https://github.com/github_username/repo_name/issues
[license-shield]: https://img.shields.io/github/license/github_username/repo_name.svg?style=for-the-badge
[license-url]: https://github.com/github_username/repo_name/blob/master/LICENSE.txt
[linkedin-shield]: https://img.shields.io/badge/-LinkedIn-black.svg?style=for-the-badge&logo=linkedin&colorB=555
[linkedin-url]: https://linkedin.com/in/linkedin_username
[product-screenshot]: images/screenshot.png
[Next.js]: https://img.shields.io/badge/next.js-000000?style=for-the-badge&logo=nextdotjs&logoColor=white
[Next-url]: https://nextjs.org/
[React.js]: https://img.shields.io/badge/React-20232A?style=for-the-badge&logo=react&logoColor=61DAFB
[React-url]: https://reactjs.org/
[Vue.js]: https://img.shields.io/badge/Vue.js-35495E?style=for-the-badge&logo=vuedotjs&logoColor=4FC08D
[Vue-url]: https://vuejs.org/
[Angular.io]: https://img.shields.io/badge/Angular-DD0031?style=for-the-badge&logo=angular&logoColor=white
[Angular-url]: https://angular.io/
[Svelte.dev]: https://img.shields.io/badge/Svelte-4A4A55?style=for-the-badge&logo=svelte&logoColor=FF3E00
[Svelte-url]: https://svelte.dev/
[Laravel.com]: https://img.shields.io/badge/Laravel-FF2D20?style=for-the-badge&logo=laravel&logoColor=white
[Laravel-url]: https://laravel.com
[Bootstrap.com]: https://img.shields.io/badge/Bootstrap-563D7C?style=for-the-badge&logo=bootstrap&logoColor=white
[Bootstrap-url]: https://getbootstrap.com
[JQuery.com]: https://img.shields.io/badge/jQuery-0769AD?style=for-the-badge&logo=jquery&logoColor=white
[JQuery-url]: https://jquery.com 